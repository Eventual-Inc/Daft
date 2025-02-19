use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fs::File,
    io::Write,
    sync::{atomic::AtomicUsize, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

use common_daft_config::DaftExecutionConfig;
use common_display::mermaid::{MermaidDisplay, MermaidDisplayOptions, SubgraphOptions};
use common_error::DaftResult;
use common_treenode::{
    DynTreeNode, Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use daft_logical_plan::{
    ops::Source,
    optimization::OptimizerBuilder,
    source_info::{InMemoryInfo, PlaceHolderInfo, SourceInfo},
    LogicalPlan, LogicalPlanRef,
};
use serde::{Deserialize, Serialize};

use super::translate::translate_single_logical_node;
use crate::{
    ops::{InMemoryScan, PlaceholderScan},
    PhysicalPlan, PhysicalPlanRef,
};

static PLACEHOLDER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
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
        let (output, _) =
            translate_single_logical_node(node, &mut self.physical_children, &self.cfg)?;
        self.physical_children.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}

pub(super) struct LogicalStageTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub root: Arc<LogicalPlan>,
    pub cfg: Arc<DaftExecutionConfig>,
    pub source_id: Option<usize>,
}

impl TreeNodeRewriter for LogicalStageTranslator {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let (translated_pplan, is_logical_boundary) =
            translate_single_logical_node(&node, &mut self.physical_children, &self.cfg)?;

        let is_root_node = Arc::ptr_eq(&node, &self.root);
        if is_logical_boundary && !is_root_node {
            log::info!(
                "Detected Query Stage Boundary at {}",
                translated_pplan.name()
            );
            match &translated_pplan.arc_children()[..] {
                [] | [_] => {
                    self.physical_children.push(translated_pplan.clone());

                    let source_id =
                        PLACEHOLDER_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                    let ph_info = PlaceHolderInfo::new(
                        source_id,
                        node.schema(),
                        translated_pplan.clustering_spec(),
                    );

                    self.source_id = Some(source_id);

                    let new_scan = LogicalPlan::Source(Source::new(
                        node.schema(),
                        SourceInfo::PlaceHolder(ph_info).into(),
                    ));
                    Ok(Transformed::new(
                        new_scan.arced(),
                        true,
                        TreeNodeRecursion::Stop,
                    ))
                }
                [left, right] => {
                    fn num_in_memory_children(plan: &PhysicalPlan) -> usize {
                        if matches!(plan, PhysicalPlan::InMemoryScan(..)) {
                            1
                        } else {
                            plan.arc_children()
                                .iter()
                                .map(|c| num_in_memory_children(c))
                                .sum()
                        }
                    }

                    enum RunNext {
                        Left,
                        Right,
                    }

                    let run_next: RunNext = match (left.as_ref(), right.as_ref()) {
                        (PhysicalPlan::InMemoryScan(..), _) => {
                            // we know the left, so let's run the right
                            RunNext::Right
                        }
                        (_, PhysicalPlan::InMemoryScan(..)) => {
                            // we know the right, so let's run the left
                            RunNext::Left
                        }
                        (_, _) => {
                            let left_num_in_memory_children = num_in_memory_children(left.as_ref());
                            let right_num_in_memory_children =
                                num_in_memory_children(right.as_ref());

                            // Bias towards the side that has more materialized children
                            match left_num_in_memory_children.cmp(&right_num_in_memory_children) {
                                Ordering::Greater => RunNext::Left,
                                Ordering::Less => RunNext::Right,
                                Ordering::Equal => {
                                    // Both sides are not in memory, so we should rank which side to run
                                    let left_stats = left.approximate_stats();
                                    let right_stats = right.approximate_stats();

                                    if left_stats.size_bytes <= right_stats.size_bytes {
                                        RunNext::Left
                                    } else {
                                        RunNext::Right
                                    }
                                }
                            }
                        }
                    };
                    match run_next {
                        RunNext::Left => {
                            self.physical_children.push(left.clone());
                            let logical_children = node.arc_children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );

                            let source_id = PLACEHOLDER_ID_COUNTER
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                            let ph_info = PlaceHolderInfo::new(
                                source_id,
                                logical_left.schema(),
                                left.clustering_spec(),
                            );

                            self.source_id = Some(source_id);

                            let new_left_scan = LogicalPlan::Source(Source::new(
                                logical_left.schema(),
                                SourceInfo::PlaceHolder(ph_info).into(),
                            ));
                            let new_bin_node = node
                                .with_new_children(&[new_left_scan.arced(), logical_right.clone()]);
                            Ok(Transformed::new(
                                new_bin_node.arced(),
                                true,
                                TreeNodeRecursion::Stop,
                            ))
                        }
                        RunNext::Right => {
                            self.physical_children.push(right.clone());
                            let logical_children = node.arc_children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );

                            let source_id = PLACEHOLDER_ID_COUNTER
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                            let ph_info = PlaceHolderInfo::new(
                                source_id,
                                logical_right.schema(),
                                right.clustering_spec(),
                            );

                            self.source_id = Some(source_id);

                            let new_right_scan = LogicalPlan::Source(Source::new(
                                logical_right.schema(),
                                SourceInfo::PlaceHolder(ph_info).into(),
                            ));
                            let new_bin_node = node
                                .with_new_children(&[logical_left.clone(), new_right_scan.arced()]);
                            Ok(Transformed::new(
                                new_bin_node.arced(),
                                true,
                                TreeNodeRecursion::Stop,
                            ))
                        }
                    }
                }
                _ => panic!("We shouldn't have any nodes that have more than 3 children"),
            }
        } else {
            self.physical_children.push(translated_pplan);
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
                    if mat_results.source_id != phi.source_id {
                        return Err(common_error::DaftError::ValueError(format!("During AQE: We are replacing a PlaceHolder Node with materialized results. There should only be 1 placeholder at a time but we found one with a different id, {} vs {}", mat_results.source_id, phi.source_id)));
                    }
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
    source_id: Option<usize>,
    root: Arc<PhysicalPlan>,
}

impl TreeNodeRewriter for PhysicalStageTranslator {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let is_root = Arc::ptr_eq(&node, &self.root);
        if is_root {
            return Ok(Transformed::no(node));
        }

        // If the node is a shuffle exchange, we have reached a physical boundary
        let shuffle_exchange = match node.as_ref() {
            PhysicalPlan::ShuffleExchange(shuffle_exchange) => shuffle_exchange,
            _ => return Ok(Transformed::no(node)),
        };

        let source_id = PLACEHOLDER_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.source_id = Some(source_id);
        // If the input is an in memory scan, emit the shuffle exchange and return a placeholder
        if matches!(
            shuffle_exchange.input.as_ref(),
            PhysicalPlan::InMemoryScan(..)
        ) {
            self.partial_physical_plan = Some(node.clone());
            let placeholder = PhysicalPlan::PlaceholderScan(PlaceholderScan::new(
                source_id,
                node.clustering_spec(),
            ));
            return Ok(Transformed::new(
                placeholder.arced(),
                true,
                TreeNodeRecursion::Stop,
            ));
        }

        // Otherwise, emit the child and add a placeholder as the child of the shuffle exchange
        let child = shuffle_exchange.input.clone();
        let placeholder =
            PhysicalPlan::PlaceholderScan(PlaceholderScan::new(source_id, child.clustering_spec()));
        self.partial_physical_plan = Some(child);
        let node_with_placeholder_child = node.with_new_children(&[placeholder.arced()]);
        Ok(Transformed::new(
            node_with_placeholder_child.arced(),
            true,
            TreeNodeRecursion::Stop,
        ))
    }
}

struct ReplacePhysicalPlaceholderWithMaterializedResults {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplacePhysicalPlaceholderWithMaterializedResults {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::PlaceholderScan(ph_scan) => {
                let mat_results = self.mat_results.take().unwrap();
                if mat_results.source_id != ph_scan.source_id {
                    return Err(common_error::DaftError::ValueError(format!("During AQE: We are replacing a PlaceHolder Node with materialized results. There should only be 1 placeholder at a time but we found one with a different id, {} vs {}", mat_results.source_id, ph_scan.source_id)));
                }
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

struct StripCacheEntryFromInMemoryScan {}

impl TreeNodeRewriter for StripCacheEntryFromInMemoryScan {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::InMemoryScan(in_memory_scan)
                if in_memory_scan.in_memory_info.source_id.is_some() =>
            {
                // new in memory scan with no partition cache entry
                let mut new_in_memory_info = in_memory_scan.in_memory_info.clone();
                new_in_memory_info.cache_entry = None;
                let new_in_memory_scan = InMemoryScan::new(
                    in_memory_scan.schema.clone(),
                    new_in_memory_info,
                    in_memory_scan.clustering_spec.clone(),
                );
                let new_physical_plan = PhysicalPlan::InMemoryScan(new_in_memory_scan);
                Ok(Transformed::new(
                    new_physical_plan.arced(),
                    true,
                    TreeNodeRecursion::Stop,
                ))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryStageOutput {
    Partial {
        physical_plan: PhysicalPlanRef,
        source_id: usize,
    },
    Final {
        physical_plan: PhysicalPlanRef,
    },
}

impl QueryStageOutput {
    pub fn source_id(&self) -> Option<usize> {
        match self {
            Self::Partial { source_id, .. } => Some(*source_id),
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

struct EmittedStage {
    pub stage_id: Option<usize>,
    pub physical_plan: PhysicalPlanRef,
    pub input_stages: Vec<EmittedStage>,
}

impl EmittedStage {
    pub fn new(
        stage_id: Option<usize>,
        physical_plan: PhysicalPlanRef,
        input_stages: Vec<EmittedStage>,
    ) -> DaftResult<Self> {
        let mut strip_cache_entry = StripCacheEntryFromInMemoryScan {};
        let physical_plan = physical_plan.rewrite(&mut strip_cache_entry)?.data;
        Ok(Self {
            stage_id,
            physical_plan,
            input_stages,
        })
    }
}

#[derive(PartialEq, Debug)]
enum AdaptivePlannerStatus {
    Ready,
    WaitingForStats,
    Done,
}

pub struct MaterializedResults {
    pub source_id: usize,
    pub in_memory_info: InMemoryInfo,
}

pub struct AdaptivePlanner {
    remaining_logical_plan: Option<LogicalPlanRef>,
    logical_source_id: Option<usize>,
    remaining_physical_plan: Option<PhysicalPlanRef>,
    emitted_stages: HashMap<usize, EmittedStage>,
    final_stage: Option<EmittedStage>,
    cfg: Arc<DaftExecutionConfig>,
    status: AdaptivePlannerStatus,
}

impl AdaptivePlanner {
    pub fn new(logical_plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        Self {
            remaining_logical_plan: Some(logical_plan),
            logical_source_id: None,
            remaining_physical_plan: None,
            emitted_stages: HashMap::new(),
            final_stage: None,
            cfg,
            status: AdaptivePlannerStatus::Ready,
        }
    }

    fn transform_physical_plan(
        &mut self,
        physical_plan: PhysicalPlanRef,
    ) -> DaftResult<(PhysicalPlanRef, Option<usize>)> {
        let mut physical_stage_translator = PhysicalStageTranslator {
            partial_physical_plan: None,
            root: physical_plan.clone(),
            source_id: self.logical_source_id,
        };
        let result = physical_plan.rewrite(&mut physical_stage_translator)?;
        if result.transformed {
            assert!(physical_stage_translator.partial_physical_plan.is_some());
            self.remaining_physical_plan = Some(result.data);
            let physical_plan = physical_stage_translator.partial_physical_plan.unwrap();
            Ok((physical_plan, physical_stage_translator.source_id))
        } else {
            assert!(physical_stage_translator.partial_physical_plan.is_none());
            let physical_plan = result.data;
            Ok((physical_plan, physical_stage_translator.source_id))
        }
    }

    fn cache_emitted_plan(&mut self, plan: QueryStageOutput) -> DaftResult<()> {
        if self.emitted_stages.is_empty() {
            if matches!(plan, QueryStageOutput::Final { .. }) {
                let emitted_stage = EmittedStage::new(None, plan.physical_plan().clone(), vec![])?;
                self.final_stage = Some(emitted_stage);
            } else {
                let emitted_stage =
                    EmittedStage::new(plan.source_id(), plan.physical_plan().clone(), vec![])?;
                self.emitted_stages
                    .insert(plan.source_id().unwrap(), emitted_stage);
            }
        }

        fn find_input_source_ids(plan: &PhysicalPlan, source_ids: &mut Vec<usize>) {
            match plan {
                PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                    if let Some(source_id) = in_memory_info.source_id {
                        source_ids.push(source_id);
                    }
                }
                _ => {
                    for child in plan.children() {
                        find_input_source_ids(child, source_ids);
                    }
                }
            }
        }
        let mut source_ids = vec![];
        find_input_source_ids(&plan.physical_plan(), &mut source_ids);
        if source_ids.is_empty() {
            let emitted_stage =
                EmittedStage::new(plan.source_id(), plan.physical_plan().clone(), vec![])?;
            if let Some(source_id) = plan.source_id() {
                self.emitted_stages.insert(source_id, emitted_stage);
            } else {
                self.final_stage = Some(emitted_stage);
            }
        } else {
            let mut input_stages = vec![];
            for source_id in source_ids {
                input_stages.push(self.emitted_stages.remove(&source_id).unwrap());
            }
            let emitted_stage =
                EmittedStage::new(plan.source_id(), plan.physical_plan().clone(), input_stages)?;
            if let Some(source_id) = plan.source_id() {
                self.emitted_stages.insert(source_id, emitted_stage);
            } else {
                self.final_stage = Some(emitted_stage);
            }
        }
        Ok(())
    }

    pub fn next_stage(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        // First, check if we have a remaining physical plan
        if let Some(remaining_physical_plan) = self.remaining_physical_plan.take() {
            let (physical_plan, source_id) =
                self.transform_physical_plan(remaining_physical_plan)?;
            // If we have no remaining logical plan and no remaining physical plan, we can emit the final plan
            if self.remaining_logical_plan.is_none() && self.remaining_physical_plan.is_none() {
                log::info!("Emitting final plan:\n {}", physical_plan.repr_ascii(true));
                self.status = AdaptivePlannerStatus::Done;
                self.cache_emitted_plan(QueryStageOutput::Final {
                    physical_plan: physical_plan.clone(),
                })?;
                return Ok(QueryStageOutput::Final { physical_plan });
            } else {
                // Otherwise, we need to emit a partial plan
                log::info!(
                    "Emitting partial plan:\n {}",
                    physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::WaitingForStats;
                self.cache_emitted_plan(QueryStageOutput::Partial {
                    physical_plan: physical_plan.clone(),
                    source_id: source_id.unwrap(),
                })?;
                return Ok(QueryStageOutput::Partial {
                    physical_plan,
                    source_id: source_id.unwrap(),
                });
            }
        }

        // If we have no remaining physical plan, we need to translate the remaining logical plan to a physical plan
        let logical_plan = self.remaining_logical_plan.take().unwrap();
        let mut logical_rewriter = LogicalStageTranslator {
            physical_children: vec![],
            root: logical_plan.clone(),
            cfg: self.cfg.clone(),
            source_id: None,
        };
        let logical_output = logical_plan.rewrite(&mut logical_rewriter)?;
        if let Some(source_id) = logical_rewriter.source_id {
            self.logical_source_id = Some(source_id);
        }
        let physical_plan = logical_rewriter
            .physical_children
            .pop()
            .expect("should have at least 1 child");
        let (transformed_physical_plan, source_id) = self.transform_physical_plan(physical_plan)?;
        // If we transformed the logical plan, this means we have a remaining logical plan
        if logical_output.transformed {
            self.remaining_logical_plan = Some(logical_output.data);
            self.status = AdaptivePlannerStatus::WaitingForStats;
            log::info!(
                "Logical plan remaining:\n {}",
                self.remaining_logical_plan
                    .as_ref()
                    .unwrap()
                    .repr_ascii(true)
            );

            log::info!(
                "Emitting partial plan:\n {}",
                transformed_physical_plan.repr_ascii(true)
            );
            self.cache_emitted_plan(QueryStageOutput::Partial {
                physical_plan: transformed_physical_plan.clone(),
                source_id: source_id.unwrap(),
            })?;
            Ok(QueryStageOutput::Partial {
                physical_plan: transformed_physical_plan,
                source_id: source_id.unwrap(),
            })
        } else {
            log::info!("Logical plan translation complete");
            if self.remaining_physical_plan.is_some() {
                log::info!(
                    "Emitting partial plan:\n {}",
                    transformed_physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::WaitingForStats;
                self.cache_emitted_plan(QueryStageOutput::Partial {
                    physical_plan: transformed_physical_plan.clone(),
                    source_id: source_id.unwrap(),
                })?;
                Ok(QueryStageOutput::Partial {
                    physical_plan: transformed_physical_plan,
                    source_id: source_id.unwrap(),
                })
            } else {
                log::info!(
                    "Emitting final plan:\n {}",
                    transformed_physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::Done;
                self.cache_emitted_plan(QueryStageOutput::Final {
                    physical_plan: transformed_physical_plan.clone(),
                })?;
                Ok(QueryStageOutput::Final {
                    physical_plan: transformed_physical_plan,
                })
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.status == AdaptivePlannerStatus::Done
    }

    pub fn update(&mut self, mat_results: MaterializedResults) -> DaftResult<()> {
        assert_eq!(self.status, AdaptivePlannerStatus::WaitingForStats);

        // If we have a remaining physical plan, we need to replace the physical placeholder with the materialized results
        if let Some(remaining_physical_plan) = self.remaining_physical_plan.take() {
            let mut rewriter = ReplacePhysicalPlaceholderWithMaterializedResults {
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

    pub fn explain_analyze(&self, explain_analyze_dir: &str) -> DaftResult<()> {
        let curr_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let file_name = format!("explain-analyze-{curr_ms}-mermaid.md");
        let mut file = File::create(std::path::Path::new(explain_analyze_dir).join(file_name))?;
        let mut s = String::new();
        let options = MermaidDisplayOptions {
            simple: false,
            bottom_up: false,
            subgraph_options: None,
        };
        let mut visitor = StageDisplayMermaidVisitor::new(&mut s, options);
        let _ = visitor.fmt(self.final_stage.as_ref().unwrap());
        writeln!(file, "```mermaid\n{}\n```", s)?;

        Ok(())
    }
}

pub struct StageDisplayMermaidVisitor<'a, W> {
    output: &'a mut W,
    options: MermaidDisplayOptions,
}

impl<'a, W> StageDisplayMermaidVisitor<'a, W> {
    pub fn new(w: &'a mut W, options: MermaidDisplayOptions) -> Self {
        Self { output: w, options }
    }
}

impl<'a, W> StageDisplayMermaidVisitor<'a, W>
where
    W: fmt::Write,
{
    fn add_node(&mut self, node: &EmittedStage) -> fmt::Result {
        let display = self.display_for_node(node)?;
        write!(self.output, "{}", display)?;
        Ok(())
    }

    fn display_for_node(&self, node: &EmittedStage) -> Result<String, fmt::Error> {
        let simple = self.options.simple;
        let bottom_up = self.options.bottom_up;
        let stage_id = self.get_node_id(node);
        let name = self.get_node_name(node);
        let subgraph_options = SubgraphOptions {
            name,
            subgraph_id: stage_id,
        };
        let display = node.physical_plan.repr_mermaid(MermaidDisplayOptions {
            simple,
            bottom_up,
            subgraph_options: Some(subgraph_options),
        });
        Ok(display)
    }

    // Get the id of a node that has already been added.
    fn get_node_id(&self, node: &EmittedStage) -> String {
        let id = match node.stage_id {
            Some(id) => id.to_string(),
            None => "final".to_string(),
        };
        format!("stage_{}", id)
    }

    fn get_node_name(&self, node: &EmittedStage) -> String {
        let id = match node.stage_id {
            Some(id) => id.to_string(),
            None => "final".to_string(),
        };
        format!("Stage {}", id)
    }

    fn add_edge(&mut self, parent: String, child: String) -> fmt::Result {
        writeln!(self.output, r#"{child} --> {parent}"#)
    }

    fn fmt_node(&mut self, node: &EmittedStage) -> fmt::Result {
        self.add_node(node)?;
        let children = &node.input_stages;
        if children.is_empty() {
            return Ok(());
        }

        for child in children {
            self.fmt_node(&child)?;
            self.add_edge(self.get_node_id(node), self.get_node_id(&child))?;
        }

        Ok(())
    }

    fn fmt(&mut self, node: &EmittedStage) -> fmt::Result {
        if let Some(SubgraphOptions { name, subgraph_id }) = &self.options.subgraph_options {
            writeln!(self.output, r#"subgraph {subgraph_id}["{name}"]"#)?;
            self.fmt_node(node)?;
            writeln!(self.output, "end")?;
        } else {
            if self.options.bottom_up {
                writeln!(self.output, "flowchart BT")?;
            } else {
                writeln!(self.output, "flowchart TD")?;
            }

            self.fmt_node(node)?;
        }
        Ok(())
    }
}
