use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor};

use crate::logical_ops::Source;
use crate::logical_optimization::Optimizer;
use crate::logical_plan::LogicalPlan;

use crate::physical_ops::InMemoryScan;
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
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeRewriter for QueryStagePhysicalPlanTranslator {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let output = translate_single_logical_node(&node, &mut self.physical_children, &self.cfg)?;
        use PhysicalPlan::*;
        let query_stage_boundary = match output.as_ref() {
            Sort(..) => true,
            _ => false,
        };

        self.physical_children.push(output.clone());
        if query_stage_boundary {
            log::warn!("Detected Query Stage Boundary at {}", output.name());
            let new_scan = LogicalPlan::Source(Source::new(
                node.schema(),
                SourceInfo::PlaceHolderInfo(PlaceHolderInfo::new(
                    node.schema(),
                    output.clustering_spec(),
                ))
                .into(),
            ));
            Ok(Transformed::new(
                new_scan.arced(),
                true,
                TreeNodeRecursion::Stop,
            ))
        } else {
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
                SourceInfo::PlaceHolderInfo(phi) => {
                    assert!(self.mat_results.is_some());
                    let mut mat_results = self.mat_results.take().unwrap();

                    // use the clustering spec from the original plan
                    mat_results.in_memory_info.clustering_spec = Some(phi.clustering_spec.clone());
                    mat_results.in_memory_info.source_schema = phi.source_schema.clone();
                    //TODO: check placeholder id here
                    let new_source_node = LogicalPlan::Source(Source {
                        output_schema: mat_results.in_memory_info.source_schema.clone(),
                        source_info: SourceInfo::InMemoryInfo(mat_results.in_memory_info).into(),
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

pub(super) enum QueryStageOutput {
    Partial { physical_plan: PhysicalPlanRef },
    Final { physical_plan: PhysicalPlanRef },
}

impl QueryStageOutput {
    pub fn unwrap(self) -> PhysicalPlanRef {
        match self {
            QueryStageOutput::Partial { physical_plan }
            | QueryStageOutput::Final { physical_plan } => physical_plan,
        }
    }
}
#[derive(PartialEq, Debug)]
enum AdaptivePlannerStatus {
    Ready,
    WaitingForStats,
    Done,
}

pub(super) struct MaterializedResults {
    pub in_memory_info: InMemoryInfo,
}

pub(super) struct AdaptivePlanner {
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

    pub fn next(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        let mut rewriter = QueryStagePhysicalPlanTranslator {
            physical_children: vec![],
            cfg: self.cfg.clone(),
        };

        let output = self.logical_plan.clone().rewrite(&mut rewriter)?;

        let physical_plan = rewriter
            .physical_children
            .pop()
            .expect("should have 1 child");

        if output.transformed {
            self.logical_plan = output.data;
            self.status = AdaptivePlannerStatus::WaitingForStats;

            log::warn!("emitting partial plan:\n {}", physical_plan.repr_indent());

            log::warn!(
                "logical plan remaining:\n {}",
                self.logical_plan.repr_indent()
            );
            Ok(QueryStageOutput::Partial { physical_plan })
        } else {
            log::warn!("emitting final plan:\n {}", physical_plan.repr_indent());

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
