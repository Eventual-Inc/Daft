use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor};
use serde::{Deserialize, Serialize};

use crate::logical_ops::Source;
use crate::logical_optimization::Optimizer;
use crate::logical_plan::LogicalPlan;

use crate::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use crate::source_info::{InMemoryInfo, PlaceHolderInfo, SourceInfo};
use crate::LogicalPlanRef;

use common_treenode::TreeNodeRecursion;

use super::translate::translate_single_logical_node;
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

pub(super) struct QueryStagePhysicalPlanTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub root: Arc<LogicalPlan>,
    pub cfg: Arc<DaftExecutionConfig>,
    pub source_id: Option<usize>,
}

fn is_query_stage_boundary(plan: &PhysicalPlan) -> bool {
    use PhysicalPlan::*;
    match plan {
        Sort(..) | HashJoin(..) | SortMergeJoin(..) | ReduceMerge(..) => {
            plan.clustering_spec().num_partitions() > 1
        }
        Aggregate(agg) => match agg.input.as_ref() {
            ReduceMerge(..) => plan.clustering_spec().num_partitions() > 1,
            _ => false,
        },
        Project(proj) => is_query_stage_boundary(&proj.input),
        _ => false,
    }
}

impl TreeNodeRewriter for QueryStagePhysicalPlanTranslator {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let translated_pplan =
            translate_single_logical_node(&node, &mut self.physical_children, &self.cfg)?;

        let is_query_stage_boundary = is_query_stage_boundary(&translated_pplan);
        let is_root_node = Arc::ptr_eq(&node, &self.root);
        if is_query_stage_boundary && !is_root_node {
            log::info!(
                "Detected Query Stage Boundary at {}",
                translated_pplan.name()
            );
            match &translated_pplan.children()[..] {
                [] | [_] => {
                    self.physical_children.push(translated_pplan.clone());

                    let ph_info =
                        PlaceHolderInfo::new(node.schema(), translated_pplan.clustering_spec());

                    assert_eq!(self.source_id, None);
                    self.source_id = Some(ph_info.source_id);

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
                    enum RunNext {
                        Parent,
                        Left,
                        Right,
                    }

                    let run_next: RunNext = match (left.as_ref(), right.as_ref()) {
                        (PhysicalPlan::InMemoryScan(..), PhysicalPlan::InMemoryScan(..)) => {
                            // both are in memory, emit as is.
                            RunNext::Parent
                        }
                        (PhysicalPlan::InMemoryScan(..), _) => {
                            // we know the left, so let's run the right
                            RunNext::Right
                        }
                        (_, PhysicalPlan::InMemoryScan(..)) => {
                            // we know the right, so let's run the left
                            RunNext::Left
                        }
                        (_, _) => {
                            // both sides are not in memory, so we should rank which side to run
                            let left_stats = left.approximate_stats();
                            let right_stats = right.approximate_stats();

                            if left_stats.lower_bound_bytes <= right_stats.lower_bound_bytes {
                                RunNext::Left
                            } else {
                                RunNext::Right
                            }
                        }
                    };
                    match run_next {
                        RunNext::Parent => {
                            self.physical_children.push(translated_pplan.clone());
                            Ok(Transformed::no(node))
                        }
                        RunNext::Left => {
                            self.physical_children.push(left.clone());
                            let logical_children = node.children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let ph_info =
                                PlaceHolderInfo::new(logical_left.schema(), left.clustering_spec());

                            assert_eq!(self.source_id, None);
                            self.source_id = Some(ph_info.source_id);

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
                            let logical_children = node.children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );

                            let ph_info = PlaceHolderInfo::new(
                                logical_right.schema(),
                                right.clustering_spec(),
                            );

                            assert_eq!(self.source_id, None);
                            self.source_id = Some(ph_info.source_id);

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
            self.physical_children.push(translated_pplan.clone());
            Ok(Transformed::no(node))
        }
    }
}

struct ReplacePlaceholdersWithMaterializedResult {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplacePlaceholdersWithMaterializedResult {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            LogicalPlan::Source(Source {
                output_schema: _,
                source_info,
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

                    let new_source_node = LogicalPlan::Source(Source {
                        output_schema: mat_results.in_memory_info.source_schema.clone(),
                        source_info: SourceInfo::InMemory(mat_results.in_memory_info).into(),
                    })
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
    pub fn unwrap(self) -> (Option<usize>, PhysicalPlanRef) {
        match self {
            QueryStageOutput::Partial {
                physical_plan,
                source_id,
            } => (Some(source_id), physical_plan),
            QueryStageOutput::Final { physical_plan } => (None, physical_plan),
        }
    }

    pub fn source_id(&self) -> Option<usize> {
        match self {
            QueryStageOutput::Partial { source_id, .. } => Some(*source_id),
            QueryStageOutput::Final { .. } => None,
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
    pub source_id: usize,
    pub in_memory_info: InMemoryInfo,
}

pub struct AdaptivePlanner {
    logical_plan: LogicalPlanRef,
    cfg: Arc<DaftExecutionConfig>,
    status: AdaptivePlannerStatus,
}

impl AdaptivePlanner {
    pub fn new(logical_plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        AdaptivePlanner {
            logical_plan,
            cfg,
            status: AdaptivePlannerStatus::Ready,
        }
    }

    pub fn next_stage(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        let mut rewriter = QueryStagePhysicalPlanTranslator {
            physical_children: vec![],
            root: self.logical_plan.clone(),
            cfg: self.cfg.clone(),
            source_id: None,
        };
        let output = self.logical_plan.clone().rewrite(&mut rewriter)?;
        let physical_plan = rewriter
            .physical_children
            .pop()
            .expect("should have at least 1 child");

        if output.transformed {
            self.logical_plan = output.data;
            self.status = AdaptivePlannerStatus::WaitingForStats;
            let source_id = rewriter.source_id.expect("If we transformed the plan, it should have a placeholder and therefore a source_id");

            log::info!(
                "Emitting partial plan:\n {}",
                physical_plan.repr_ascii(true)
            );

            log::info!(
                "Logical plan remaining:\n {}",
                self.logical_plan.repr_ascii(true)
            );
            Ok(QueryStageOutput::Partial {
                physical_plan,
                source_id,
            })
        } else {
            log::info!("Emitting final plan:\n {}", physical_plan.repr_ascii(true));

            self.status = AdaptivePlannerStatus::Done;
            Ok(QueryStageOutput::Final { physical_plan })
        }
    }

    pub fn is_done(&self) -> bool {
        self.status == AdaptivePlannerStatus::Done
    }

    pub fn update(&mut self, mat_results: MaterializedResults) -> DaftResult<()> {
        assert_eq!(self.status, AdaptivePlannerStatus::WaitingForStats);

        let mut rewriter = ReplacePlaceholdersWithMaterializedResult {
            mat_results: Some(mat_results),
        };
        let result = self.logical_plan.clone().rewrite(&mut rewriter)?;

        assert!(result.transformed);

        self.logical_plan = result.data;

        let optimizer = Optimizer::new(Default::default());

        self.logical_plan = optimizer.optimize(
            self.logical_plan.clone(),
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
        self.status = AdaptivePlannerStatus::Ready;

        Ok(())
    }
}
