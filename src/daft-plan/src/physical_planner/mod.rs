use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::TreeNode;

use crate::{
    physical_optimization::optimizer::PhysicalOptimizer, physical_plan::PhysicalPlanRef,
    physical_planner::planner::PhysicalPlanTranslator, LogicalPlan,
};
mod planner;
pub use planner::{AdaptivePlanner, MaterializedResults, QueryStageOutput};
mod translate;
pub use translate::populate_aggregation_stages;

/// Translate a logical plan to a physical plan.
pub fn logical_to_physical(
    logical_plan: Arc<LogicalPlan>,
    cfg: Arc<DaftExecutionConfig>,
) -> DaftResult<PhysicalPlanRef> {
    let mut visitor = PhysicalPlanTranslator {
        physical_children: vec![],
        cfg,
    };
    let _output = logical_plan.visit(&mut visitor)?;
    assert_eq!(
        visitor.physical_children.len(),
        1,
        "We should have exactly 1 node left"
    );
    let pplan = visitor
        .physical_children
        .pop()
        .expect("should have exactly 1 parent");
    let optimizer = PhysicalOptimizer::default();
    let pplan = optimizer.optimize(pplan)?;
    Ok(pplan)
}
