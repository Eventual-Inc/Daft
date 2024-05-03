use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;

use crate::physical_plan::PhysicalPlanRef;
use crate::LogicalPlan;

use crate::physical_planner::planner::{PhysicalPlanTranslator, QueryStagePhysicalPlanTranslator};
use common_treenode::TreeNode;
mod planner;
#[cfg(feature = "python")]
pub mod python;
mod translate;

/// Translate a logical plan to a physical plan.
pub fn plan(
    logical_plan: Arc<LogicalPlan>,
    cfg: Arc<DaftExecutionConfig>,
) -> DaftResult<PhysicalPlanRef> {
    let mut visitor = PhysicalPlanTranslator {
        physical_children: vec![],
        cfg: cfg.clone(),
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
    Ok(pplan)
}

pub fn plan_new(
    logical_plan: Arc<LogicalPlan>,
    cfg: Arc<DaftExecutionConfig>,
) -> DaftResult<PhysicalPlanRef> {
    let mut rewriter = QueryStagePhysicalPlanTranslator {
        physical_children: vec![],
        root: logical_plan.clone(),
        cfg: cfg.clone(),
    };
    let _output = logical_plan.rewrite(&mut rewriter)?;
    assert_eq!(
        rewriter.physical_children.len(),
        1,
        "We should have exactly 1 node left"
    );
    let pplan = rewriter
        .physical_children
        .pop()
        .expect("should have exactly 1 parent");
    Ok(pplan)
}
