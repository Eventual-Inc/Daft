use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNodeRewriter, TreeNodeVisitor};

use crate::logical_ops::Source;
use crate::logical_plan::LogicalPlan;

use crate::physical_plan::PhysicalPlan;
use crate::source_info::{PlaceHolderInfo, SourceInfo};

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