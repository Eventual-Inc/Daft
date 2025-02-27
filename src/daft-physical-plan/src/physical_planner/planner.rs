use std::{
    collections::HashMap,
    fmt::{self, Display},
    fs::File,
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{mermaid::MermaidDisplayOptions, utils::bytes_to_human_readable};
use common_error::DaftResult;
use common_treenode::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use daft_logical_plan::{
    ops::Source,
    optimization::OptimizerBuilder,
    source_info::{InMemoryInfo, SourceInfo},
    LogicalPlan, LogicalPlanRef,
};
use serde::{Deserialize, Serialize};

use super::{
    display::StageDisplayMermaidVisitor,
    translate::{adaptively_translate_single_logical_node, translate_single_logical_node},
};
use crate::{
    ops::{InMemoryScan, PreviousStageScan},
    PhysicalPlan, PhysicalPlanRef,
};

static STAGE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) struct PhysicalPlanTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeVisitor for PhysicalPlanTranslator {
    type Node = Arc<LogicalPlan>;
    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        let output = translate_single_logical_node(node, &mut self.physical_children, &self.cfg)?;
        self.physical_children.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}

pub(super) struct LogicalStageTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeRewriter for LogicalStageTranslator {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let (translated_pplan, boundary_placeholder) = adaptively_translate_single_logical_node(
            &node,
            &mut self.physical_children,
            &self.cfg,
        )?;

        self.physical_children.push(translated_pplan);
        if let Some(boundary_placeholder) = boundary_placeholder {
            Ok(Transformed::new(
                boundary_placeholder.arced(),
                true,
                TreeNodeRecursion::Stop,
            ))
        } else {
            Ok(Transformed::no(node))
        }
    }
}

struct ReplaceLogicalPlaceholderWithMaterializedResults {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplaceLogicalPlaceholderWithMaterializedResults {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            LogicalPlan::Source(Source {
                output_schema: _,
                source_info,
                ..
            }) => match source_info.as_ref() {
                SourceInfo::PlaceHolder(phi) => {
                    assert!(self.mat_results.is_some());
                    let mut mat_results = self.mat_results.take().unwrap();
                    // use the clustering spec from the original plan
                    mat_results.in_memory_info.clustering_spec = Some(phi.clustering_spec.clone());
                    mat_results.in_memory_info.source_schema = phi.source_schema.clone();

                    let new_source_node = LogicalPlan::Source(Source::new(
                        mat_results.in_memory_info.source_schema.clone(),
                        SourceInfo::InMemory(mat_results.in_memory_info).into(),
                    ))
                    .arced();
                    Ok(Transformed::new(
                        new_source_node,
                        true,
                        TreeNodeRecursion::Stop,
                    ))
                }
                _ => Ok(Transformed::no(node)),
            },
            _ => Ok(Transformed::no(node)),
        }
    }
}

struct PhysicalStageTranslator {
    partial_physical_plan: Option<PhysicalPlanRef>,
    root: Arc<PhysicalPlan>,
}

impl TreeNodeRewriter for PhysicalStageTranslator {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        // If not a shuffle exchange, don't transform
        let shuffle_exchange = match node.as_ref() {
            PhysicalPlan::ShuffleExchange(se) => se,
            _ => return Ok(Transformed::no(node)),
        };

        // If child is not an in memory scan, emit the child
        if !matches!(
            shuffle_exchange.input.as_ref(),
            PhysicalPlan::InMemoryScan(..)
        ) {
            let child = shuffle_exchange.input.clone();
            self.partial_physical_plan = Some(child.clone());

            let placeholder =
                PhysicalPlan::PreviousStageScan(PreviousStageScan::new(child.clustering_spec()));

            return Ok(Transformed::new(
                node.with_new_children(&[placeholder.arced()]).arced(),
                true,
                TreeNodeRecursion::Stop,
            ));
        }

        // If it's a root node with in memory scan child, don't transform
        let is_root = Arc::ptr_eq(&node, &self.root);
        if is_root {
            return Ok(Transformed::no(node));
        }

        // Otherwise emit the shuffle exchange and return transformed placeholder
        self.partial_physical_plan = Some(node.clone());

        let placeholder =
            PhysicalPlan::PreviousStageScan(PreviousStageScan::new(node.clustering_spec()));

        Ok(Transformed::new(
            placeholder.arced(),
            true,
            TreeNodeRecursion::Stop,
        ))
    }
}

struct ReplacePreviousStageScanWithInMemoryScan {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplacePreviousStageScanWithInMemoryScan {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::PreviousStageScan(ph_scan) => {
                let mat_results = self.mat_results.take().unwrap();
                let new_source_node = PhysicalPlan::InMemoryScan(InMemoryScan::new(
                    mat_results.in_memory_info.source_schema.clone(),
                    mat_results.in_memory_info,
                    ph_scan.clustering_spec().clone(),
                ));
                Ok(Transformed::new(
                    new_source_node.arced(),
                    true,
                    TreeNodeRecursion::Stop,
                ))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

// Strip the cache entry from the in memory scan, so that we don't hold a reference to the cache entry for plans that we cache for explain purposes
struct StripCacheEntryFromInMemoryScan {}

impl TreeNodeRewriter for StripCacheEntryFromInMemoryScan {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::InMemoryScan(in_memory_scan) => {
                // new in memory scan with no partition cache entry
                let mut new_in_memory_info = in_memory_scan.in_memory_info.clone();
                new_in_memory_info.cache_entry = None;
                let new_in_memory_scan = InMemoryScan::new(
                    in_memory_scan.schema.clone(),
                    new_in_memory_info,
                    in_memory_scan.clustering_spec.clone(),
                );
                let new_physical_plan = PhysicalPlan::InMemoryScan(new_in_memory_scan);
                Ok(Transformed::yes(new_physical_plan.arced()))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

fn num_in_memory_children(plan: &PhysicalPlan) -> usize {
    if matches!(plan, PhysicalPlan::InMemoryScan(..)) {
        1
    } else {
        plan.children()
            .iter()
            .map(|c| num_in_memory_children(c))
            .sum()
    }
}

// Check that the physical plan has no cache entries after stripping them
fn has_cache_entries(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
            in_memory_info.cache_entry.is_some()
        }
        _ => plan.children().iter().any(|child| has_cache_entries(child)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryStageOutput {
    Partial {
        physical_plan: PhysicalPlanRef,
        stage_id: Option<usize>,
    },
    Final {
        physical_plan: PhysicalPlanRef,
    },
}

impl QueryStageOutput {
    pub fn stage_id(&self) -> Option<usize> {
        match self {
            Self::Partial { stage_id, .. } => *stage_id,
            Self::Final { .. } => None,
        }
    }

    pub fn physical_plan(&self) -> &PhysicalPlanRef {
        match self {
            Self::Partial { physical_plan, .. } => physical_plan,
            Self::Final { physical_plan, .. } => physical_plan,
        }
    }
}

#[derive(PartialEq, Debug)]
enum AdaptivePlannerStatus {
    Ready,
    WaitingForStats,
    Done,
}

pub struct MaterializedResults {
    pub stage_id: usize,
    pub in_memory_info: InMemoryInfo,
}

pub struct StageStats {
    pub time_taken: Duration,
    pub size_bytes: Option<usize>,
    pub num_rows: Option<usize>,
}

impl Display for StageStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use num_format::{Locale, ToFormattedString};
        write!(f, "Time taken: {:.2}s", self.time_taken.as_secs_f64())?;
        if let Some(size) = self.size_bytes {
            write!(f, "\nBytes processed: {}", bytes_to_human_readable(size))?;
        }
        if let Some(rows) = self.num_rows {
            write!(
                f,
                "\nRows processed: {}",
                rows.to_formatted_string(&Locale::en)
            )?;
        }

        Ok(())
    }
}

pub struct EmittedStage {
    pub stage_id: Option<usize>,
    pub physical_plan: PhysicalPlanRef,
    pub input_stages: Vec<Self>,
    pub stats: Option<StageStats>,
}

impl EmittedStage {
    fn new(query_stage_output: &QueryStageOutput, input_stages: Vec<Self>) -> DaftResult<Self> {
        let mut strip_cache_entry = StripCacheEntryFromInMemoryScan {};
        let physical_plan = query_stage_output
            .physical_plan()
            .clone()
            .rewrite(&mut strip_cache_entry)?
            .data;

        assert!(
            !has_cache_entries(&physical_plan),
            "Should not have cache entries in an emitted stage"
        );
        Ok(Self {
            stage_id: query_stage_output.stage_id(),
            physical_plan,
            input_stages,
            stats: None,
        })
    }
}

enum StageCache {
    Intermediate {
        intermediate_stages: HashMap<usize, EmittedStage>,
    },
    Final {
        final_stage: EmittedStage,
    },
}

impl StageCache {
    pub fn new() -> Self {
        Self::Intermediate {
            intermediate_stages: HashMap::new(),
        }
    }

    pub fn insert_stage(&mut self, stage: &QueryStageOutput) -> DaftResult<()> {
        let intermediate_stages = match self {
            Self::Intermediate {
                intermediate_stages,
            } => intermediate_stages,
            Self::Final { .. } => panic!("Cannot insert stage if we already have a final stage"),
        };

        fn find_input_stage_ids(plan: &PhysicalPlan, stage_ids: &mut Vec<usize>) {
            match plan {
                PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                    if let Some(stage_id) = in_memory_info.source_stage_id {
                        stage_ids.push(stage_id);
                    }
                }
                _ => {
                    for child in plan.children() {
                        find_input_stage_ids(child, stage_ids);
                    }
                }
            }
        }
        let mut stage_ids = vec![];
        find_input_stage_ids(stage.physical_plan(), &mut stage_ids);

        if stage_ids.is_empty() {
            let emitted_stage = EmittedStage::new(stage, vec![])?;
            if let Some(stage_id) = stage.stage_id() {
                intermediate_stages.insert(stage_id, emitted_stage);
            } else {
                *self = Self::Final {
                    final_stage: emitted_stage,
                };
            }
        } else {
            let input_stages = stage_ids
                .iter()
                .map(|stage_id| intermediate_stages.remove(stage_id).unwrap())
                .collect();
            let emitted_stage = EmittedStage::new(stage, input_stages)?;
            if let Some(stage_id) = stage.stage_id() {
                intermediate_stages.insert(stage_id, emitted_stage);
            } else {
                *self = Self::Final {
                    final_stage: emitted_stage,
                };
            }
        }
        Ok(())
    }

    pub fn set_stats(&mut self, stage_id: Option<usize>, stats: StageStats) -> DaftResult<()> {
        match self {
            Self::Intermediate {
                intermediate_stages,
            } => {
                intermediate_stages
                    .get_mut(&stage_id.unwrap())
                    .unwrap()
                    .stats = Some(stats);
            }
            Self::Final { final_stage } => {
                final_stage.stats = Some(stats);
            }
        }
        Ok(())
    }

    pub fn final_stage(&self) -> Option<&EmittedStage> {
        match self {
            Self::Final { final_stage } => Some(final_stage),
            _ => None,
        }
    }
}

pub struct AdaptivePlanner {
    remaining_logical_plan: Option<LogicalPlanRef>,
    remaining_physical_plan: Option<PhysicalPlanRef>,
    last_stage_id: usize,
    stage_cache: StageCache,
    cfg: Arc<DaftExecutionConfig>,
    status: AdaptivePlannerStatus,
}

impl AdaptivePlanner {
    pub fn new(logical_plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        Self {
            remaining_logical_plan: Some(logical_plan),
            remaining_physical_plan: None,
            last_stage_id: 0,
            stage_cache: StageCache::new(),
            cfg,
            status: AdaptivePlannerStatus::Ready,
        }
    }

    fn transform_physical_plan(
        &self,
        physical_plan: PhysicalPlanRef,
    ) -> DaftResult<(PhysicalPlanRef, Option<PhysicalPlanRef>)> {
        let mut physical_stage_translator = PhysicalStageTranslator {
            partial_physical_plan: None,
            root: physical_plan.clone(),
        };
        let result = physical_plan.rewrite(&mut physical_stage_translator)?;
        if result.transformed {
            assert!(physical_stage_translator.partial_physical_plan.is_some());
            let physical_plan = physical_stage_translator.partial_physical_plan.unwrap();
            Ok((physical_plan, Some(result.data)))
        } else {
            assert!(physical_stage_translator.partial_physical_plan.is_none());
            let physical_plan = result.data;
            Ok((physical_plan, None))
        }
    }

    pub fn next_stage(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        // First, check if we have a remaining physical plan
        let next_physical_plan = match self.remaining_physical_plan.take() {
            Some(remaining_physical_plan) => {
                let (physical_plan, remaining_physical_plan) =
                    self.transform_physical_plan(remaining_physical_plan)?;
                self.remaining_physical_plan = remaining_physical_plan;

                physical_plan
            }
            None => {
                // If we have no remaining physical plan, we need to translate the remaining logical plan to a physical plan
                let logical_plan = self.remaining_logical_plan.take().unwrap();
                let mut logical_rewriter = LogicalStageTranslator {
                    physical_children: vec![],
                    cfg: self.cfg.clone(),
                };
                let logical_output = logical_plan.rewrite(&mut logical_rewriter)?;

                // If we transformed the logical plan, this means we have a remaining logical plan
                if logical_output.transformed {
                    self.remaining_logical_plan = Some(logical_output.data);
                }
                let physical_plan = logical_rewriter
                    .physical_children
                    .pop()
                    .expect("should have at least 1 child");

                // Now, transform the new physical plan
                let (physical_plan, remaining_physical_plan) =
                    self.transform_physical_plan(physical_plan)?;
                self.remaining_physical_plan = remaining_physical_plan;

                physical_plan
            }
        };

        let next_stage =
            if self.remaining_physical_plan.is_some() || self.remaining_logical_plan.is_some() {
                let stage_id = STAGE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.last_stage_id = stage_id;
                QueryStageOutput::Partial {
                    physical_plan: next_physical_plan,
                    stage_id: Some(stage_id),
                }
            } else {
                QueryStageOutput::Final {
                    physical_plan: next_physical_plan,
                }
            };

        self.stage_cache.insert_stage(&next_stage)?;
        match &next_stage {
            QueryStageOutput::Final { physical_plan } => {
                log::info!("Emitting final plan:\n {}", physical_plan.repr_ascii(true));
                self.status = AdaptivePlannerStatus::Done;
            }
            QueryStageOutput::Partial { physical_plan, .. } => {
                log::info!(
                    "Emitting partial plan:\n {}",
                    physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::WaitingForStats;
            }
        }
        if num_in_memory_children(next_stage.physical_plan()) > 1 {
            assert!(
                has_cache_entries(next_stage.physical_plan()),
                "Next stage must have cache entries in in-memory scans"
            );
        }
        Ok(next_stage)
    }

    pub fn is_done(&self) -> bool {
        self.status == AdaptivePlannerStatus::Done
    }

    pub fn update(&mut self, mat_results: MaterializedResults) -> DaftResult<()> {
        assert_eq!(self.status, AdaptivePlannerStatus::WaitingForStats);
        assert!(mat_results.stage_id == self.last_stage_id);

        // If we have a remaining physical plan, we need to replace the physical previous stage scan with the materialized results
        if let Some(remaining_physical_plan) = self.remaining_physical_plan.take() {
            let mut rewriter = ReplacePreviousStageScanWithInMemoryScan {
                mat_results: Some(mat_results),
            };
            let result = remaining_physical_plan.rewrite(&mut rewriter)?;

            assert!(result.transformed);

            self.remaining_physical_plan = Some(result.data);
        }
        // Otherwise, we need to replace the logical placeholder with the materialized results
        else {
            let mut rewriter = ReplaceLogicalPlaceholderWithMaterializedResults {
                mat_results: Some(mat_results),
            };
            let logical_plan = self.remaining_logical_plan.take().unwrap();
            let result = logical_plan.rewrite(&mut rewriter)?;

            assert!(result.transformed);

            let optimizer = OptimizerBuilder::new().enrich_with_stats().build();

            let optimized_logical_plan = optimizer.optimize(
                result.data,
                |new_plan, rule_batch, pass, transformed, seen| {
                    if transformed {
                        log::debug!(
                        "Rule batch {:?} transformed plan on pass {}, and produced {} plan:\n{}",
                        rule_batch,
                        pass,
                        if seen { "an already seen" } else { "a new" },
                        new_plan.repr_ascii(true),
                    );
                    } else {
                        log::debug!(
                            "Rule batch {:?} did NOT transform plan on pass {} for plan:\n{}",
                            rule_batch,
                            pass,
                            new_plan.repr_ascii(true),
                        );
                    }
                },
            )?;

            self.remaining_logical_plan = Some(optimized_logical_plan);
        }

        self.status = AdaptivePlannerStatus::Ready;
        Ok(())
    }

    pub fn update_stats(&mut self, stats: StageStats, stage_id: Option<usize>) -> DaftResult<()> {
        self.stage_cache.set_stats(stage_id, stats)?;
        Ok(())
    }

    pub fn explain_analyze(&mut self, explain_analyze_dir: &str) -> DaftResult<()> {
        let curr_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let file_name = format!("explain-analyze-{curr_ms}-mermaid.md");
        let path = std::path::Path::new(explain_analyze_dir).join(file_name);
        let mut file = File::create(path.clone())?;
        let mut s = String::new();
        let options = MermaidDisplayOptions {
            simple: false,
            bottom_up: true,
            subgraph_options: None,
        };
        let mut visitor = StageDisplayMermaidVisitor::new(&mut s, options);
        let _ = visitor.fmt(
            self.stage_cache
                .final_stage()
                .expect("Explain analyze should have a final stage"),
        );
        writeln!(file, "```mermaid\n{}\n```", s)?;

        println!("Exported explain analyze to {}", path.display());
        Ok(())
    }
}
