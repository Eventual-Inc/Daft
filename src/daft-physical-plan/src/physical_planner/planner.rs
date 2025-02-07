use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use daft_logical_plan::{InMemoryInfo, LogicalPlan};
use serde::{Deserialize, Serialize};

use super::translate::translate_single_logical_node;
use crate::{
    ops::{InMemoryScan, PlaceholderScan},
    PhysicalPlan, PhysicalPlanRef,
};

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
    pub result: Option<PhysicalPlanRef>,
    pub root: PhysicalPlanRef,
    pub source_id: Option<usize>,
}

impl TreeNodeRewriter for QueryStagePhysicalPlanTranslator {
    type Node = PhysicalPlanRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let is_root_node = Arc::ptr_eq(&node, &self.root);
        if is_root_node {
            self.result = Some(node.clone());
            return Ok(Transformed::no(node));
        }

        let is_query_stage_boundary: bool =
            matches!(node.as_ref(), PhysicalPlan::ShuffleExchange(..));
        if is_query_stage_boundary {
            let placeholder = PlaceholderScan::new(node.clustering_spec());
            self.result = Some(node);
            self.source_id = Some(placeholder.source_id());
            let new_scan = PhysicalPlan::PlaceholderScan(placeholder).arced();
            Ok(Transformed::new(new_scan, true, TreeNodeRecursion::Stop))
        } else {
            Ok(Transformed::no(node))
        }
    }
}

struct ReplacePlaceholdersWithMaterializedResult {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplacePlaceholdersWithMaterializedResult {
    type Node = PhysicalPlanRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::PlaceholderScan(placeholder) => {
                assert!(self.mat_results.is_some());
                let mut mat_results = self.mat_results.take().unwrap();
                if mat_results.source_id != placeholder.source_id() {
                    return Err(common_error::DaftError::ValueError(format!(
                        "During AQE: We are replacing a PlaceHolder Node with materialized results. There should only be 1 placeholder at a time but we found one with a different id, {} vs {}",
                        mat_results.source_id,
                        placeholder.source_id()
                    )));
                }

                mat_results.in_memory_info.clustering_spec =
                    Some(placeholder.clustering_spec().clone());
                let in_memory_scan = InMemoryScan::new(
                    mat_results.in_memory_info.source_schema.clone(),
                    mat_results.in_memory_info,
                    placeholder.clustering_spec().clone(),
                );
                let new_source_node = PhysicalPlan::InMemoryScan(in_memory_scan).arced();
                Ok(Transformed::new(
                    new_source_node,
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
    pub fn unwrap(self) -> (Option<usize>, PhysicalPlanRef) {
        match self {
            Self::Partial {
                physical_plan,
                source_id,
            } => (Some(source_id), physical_plan),
            Self::Final { physical_plan } => (None, physical_plan),
        }
    }

    pub fn source_id(&self) -> Option<usize> {
        match self {
            Self::Partial { source_id, .. } => Some(*source_id),
            Self::Final { .. } => None,
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
    physical_plan: PhysicalPlanRef,
    status: AdaptivePlannerStatus,
}

impl AdaptivePlanner {
    pub fn new(physical_plan: PhysicalPlanRef) -> Self {
        Self {
            physical_plan,
            status: AdaptivePlannerStatus::Ready,
        }
    }

    pub fn next_stage(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        let mut rewriter = QueryStagePhysicalPlanTranslator {
            result: None,
            root: self.physical_plan.clone(),
            source_id: None,
        };
        let output = self.physical_plan.clone().rewrite(&mut rewriter)?;
        let physical_plan = rewriter
            .result
            .expect("should have a result after rewriting the plan");

        if output.transformed {
            self.physical_plan = output.data;
            self.status = AdaptivePlannerStatus::WaitingForStats;
            let source_id = rewriter.source_id.expect("If we transformed the plan, it should have a placeholder and therefore a source_id");

            log::info!(
                "Emitting partial plan:\n {}",
                physical_plan.repr_ascii(true)
            );

            log::info!(
                "Physical plan remaining:\n {}",
                self.physical_plan.repr_ascii(true)
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
        let result = self.physical_plan.clone().rewrite(&mut rewriter)?;

        assert!(result.transformed);

        self.physical_plan = result.data;
        self.status = AdaptivePlannerStatus::Ready;

        Ok(())
    }
}
