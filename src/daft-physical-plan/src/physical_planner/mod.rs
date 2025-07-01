use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::TreeNode;
use daft_logical_plan::LogicalPlan;
mod planner;

use planner::PhysicalPlanTranslator;
pub use planner::{AdaptivePlanner, MaterializedResults, QueryStageOutput, StageStats};

use crate::{optimization::optimizer::PhysicalOptimizer, PhysicalPlanRef};
mod translate;
pub use translate::{
    extract_agg_expr, populate_aggregation_stages, populate_aggregation_stages_bound,
    populate_aggregation_stages_bound_with_schema,
};

mod display;

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
