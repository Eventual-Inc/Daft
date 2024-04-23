use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::TreeNodeVisitor;

use crate::logical_plan::LogicalPlan;

use crate::physical_plan::PhysicalPlan;

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
