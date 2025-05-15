use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use common_error::DaftResult;
use common_scan_info::ScanState;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, LogicalPlan, LogicalPlanRef, SourceInfo,
};
use serde::{Deserialize, Serialize};

use super::PipelineInput;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct PipelinePlan {
    pub local_plan: LocalPhysicalPlanRef,
    pub input: PipelineInput,
}

#[allow(dead_code)]
pub(crate) fn translate_logical_plan_to_pipeline_plan(
    logical_plan: LogicalPlanRef,
) -> DaftResult<PipelinePlan> {
    let mut pipeline_input = None;
    let transformed_plan = logical_plan.transform_up(|plan| match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::InMemory(info) => {
                pipeline_input = Some(PipelineInput::InMemorySource { info: info.clone() });
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop))
            }
            SourceInfo::Physical(info) => {
                let pushdowns = info.pushdowns.clone();
                let scan_tasks = match &info.scan_state {
                    ScanState::Tasks(tasks) => tasks.clone(),
                    ScanState::Operator(_) => unreachable!(),
                };
                pipeline_input = Some(PipelineInput::ScanTasks {
                    pushdowns,
                    scan_tasks,
                });
                let placeholder =
                    PlaceHolderInfo::new(source.output_schema.clone(), Default::default());
                let placeholder_source = LogicalPlan::Source(Source::new(
                    source.output_schema.clone(),
                    Arc::new(SourceInfo::PlaceHolder(placeholder)),
                ));
                Ok(Transformed::new(
                    placeholder_source.into(),
                    true,
                    TreeNodeRecursion::Stop,
                ))
            }
            SourceInfo::PlaceHolder(_) => {
                pipeline_input = Some(PipelineInput::Intermediate);
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop))
            }
        },
        _ => Ok(Transformed::no(plan)),
    })?;

    let plan = transformed_plan.data;
    let translated = translate(&plan)?;
    let inputs = pipeline_input.expect("Should have inputs");

    Ok(PipelinePlan {
        local_plan: translated,
        input: inputs,
    })
}
