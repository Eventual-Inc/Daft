use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlanRef;
use daft_logical_plan::LogicalPlanRef;

#[allow(dead_code)]
pub(crate) fn translate_pipeline_plan_to_local_physical_plans(
    _logical_plan: LogicalPlanRef,
    _execution_config: &DaftExecutionConfig,
) -> DaftResult<Vec<LocalPhysicalPlanRef>> {
    todo!("FLOTILLA_MS1: Implement translate pipeline plan to local physical plans");
}
