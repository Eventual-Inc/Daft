use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::TreeNodeVisitor;

use crate::logical_plan::LogicalPlan;

use crate::physical_plan::PhysicalPlan;

use common_treenode::VisitRecursion;

use super::translate::translate_single_logical_node;
pub(super) struct PhysicalPlanTranslator {
    pub physical_children: Vec<PhysicalPlan>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeVisitor for PhysicalPlanTranslator {
    type N = LogicalPlan;
    fn pre_visit(&mut self, _node: &Self::N) -> DaftResult<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(&mut self, node: &Self::N) -> DaftResult<VisitRecursion> {
        let output = translate_single_logical_node(node, &mut self.physical_children, &self.cfg)?;
        self.physical_children.push(output);
        Ok(VisitRecursion::Continue)
    }
}
