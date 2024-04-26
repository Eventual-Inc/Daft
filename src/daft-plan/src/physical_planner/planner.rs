use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor};

use crate::logical_ops::Source;
use crate::logical_plan::LogicalPlan;

use crate::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use crate::source_info::{PlaceHolderInfo, SourceInfo};
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
            _ => false
        };

        self.physical_children.push(output.clone());
        if query_stage_boundary {
            log::warn!("Detected Query Stage Boundary at {}", output.name());
            let new_scan = LogicalPlan::Source(Source::new(node.schema(), SourceInfo::PlaceHolderInfo(PlaceHolderInfo::new(node.schema(), output.clustering_spec())).into()));
            Transformed::new(new_scan, true, TreeNodeRecursion::Stop);
        }
        Ok(Transformed::no(node))

    }
}

enum QueryStageOutput {
    Partial{physical_plan: PhysicalPlanRef },
    Final{physical_plan: PhysicalPlanRef}
}

#[derive(PartialEq, Debug)]
enum AdaptivePlannerStatus {
    Ready,
    WaitingForStats,
    Done
}


struct AdaptivePlanner {
    logical_plan: LogicalPlanRef,
    cfg: Arc<DaftExecutionConfig>,
    status: AdaptivePlannerStatus
}

impl AdaptivePlanner {
    pub fn new(logical_plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        AdaptivePlanner { logical_plan, cfg, status: AdaptivePlannerStatus::Ready}
    }

    pub fn next(&mut self) -> DaftResult<QueryStageOutput> {

        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        let mut rewriter = QueryStagePhysicalPlanTranslator {
            physical_children: vec![],
            cfg: self.cfg.clone(),
        };

        let output = self.logical_plan.clone().rewrite(&mut rewriter)?;

        let physical_plan = rewriter.physical_children.pop().expect("should have 1 child");

        if output.transformed {
            self.logical_plan = output.data;
            self.status = AdaptivePlannerStatus::WaitingForStats;
            Ok(QueryStageOutput::Partial { physical_plan })
        } else {
            self.status = AdaptivePlannerStatus::Done;
            Ok(QueryStageOutput::Final { physical_plan })
        }
    }


    pub fn update(&mut self) -> DaftResult<()> {
        assert_eq!(self.status, AdaptivePlannerStatus::WaitingForStats);

        Ok(())
    }
}
