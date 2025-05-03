use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, ScanState};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::{Explode, Filter, Project, Sample, Sink, Source, Unpivot},
    optimization::OptimizerBuilder,
    LogicalPlan, LogicalPlanRef, SourceInfo,
};

#[allow(dead_code)]
pub(crate) fn translate_pipeline_plan_to_local_physical_plans(
    logical_plan: LogicalPlanRef,
    execution_config: &DaftExecutionConfig,
) -> DaftResult<Vec<LocalPhysicalPlanRef>> {
    // This function is used to split the logical plan into multiple logical plans,
    // based on the source info.
    fn create_logical_plan_splits(
        logical_plan: LogicalPlanRef,
        execution_config: &DaftExecutionConfig,
        logical_plan_splits: &mut Vec<Vec<LogicalPlanRef>>,
    ) -> DaftResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                SourceInfo::InMemory(_) => {
                    logical_plan_splits.push(vec![logical_plan]);
                }
                SourceInfo::Physical(info) => {
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    if scan_tasks.is_empty() {
                        logical_plan_splits.push(vec![logical_plan]);
                    } else {
                        let mut plans = Vec::new();
                        let mut scan_tasks_accumulator = Vec::new();
                        let mut scan_tasks_bytes_so_far = 0;
                        for scan_task in scan_tasks.iter() {
                            scan_tasks_bytes_so_far += scan_task
                                .estimate_in_memory_size_bytes(Some(execution_config))
                                .unwrap_or(0);
                            if scan_tasks_bytes_so_far > execution_config.scan_tasks_max_size_bytes
                                && !scan_tasks_accumulator.is_empty()
                            {
                                let plan = LogicalPlan::Source(Source::new(
                                    source.output_schema.clone(),
                                    Arc::new(SourceInfo::Physical(PhysicalScanInfo {
                                        scan_state: ScanState::Tasks(
                                            std::mem::take(&mut scan_tasks_accumulator).into(),
                                        ),
                                        source_schema: source.output_schema.clone(),
                                        partitioning_keys: info.partitioning_keys.clone(),
                                        pushdowns: info.pushdowns.clone(),
                                    })),
                                ));
                                plans.push(plan.into());
                                scan_tasks_bytes_so_far = 0;
                            }
                            scan_tasks_accumulator.push(scan_task.clone());
                        }
                        if !scan_tasks_accumulator.is_empty() {
                            let plan = LogicalPlan::Source(Source::new(
                                source.output_schema.clone(),
                                Arc::new(SourceInfo::Physical(PhysicalScanInfo {
                                    scan_state: ScanState::Tasks(scan_tasks_accumulator.into()),
                                    source_schema: source.output_schema.clone(),
                                    partitioning_keys: info.partitioning_keys.clone(),
                                    pushdowns: info.pushdowns.clone(),
                                })),
                            ));
                            plans.push(plan.into());
                        }
                        logical_plan_splits.push(plans);
                    }
                }
                SourceInfo::PlaceHolder(_) => {
                    panic!("We should not encounter a PlaceHolder during translation")
                }
            },
            LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::Project(Project { input, .. })
            | LogicalPlan::Unpivot(Unpivot { input, .. })
            | LogicalPlan::Explode(Explode { input, .. })
            | LogicalPlan::Sink(Sink { input, .. })
            | LogicalPlan::Sample(Sample { input, .. }) => {
                create_logical_plan_splits(input.clone(), execution_config, logical_plan_splits)?;
                assert!(logical_plan_splits.len() == 1);
                let plans = logical_plan_splits.pop().unwrap();
                let new_plans = plans
                    .into_iter()
                    .map(|plan| logical_plan.with_new_children(&[plan]).into())
                    .collect();
                logical_plan_splits.push(new_plans);
            }
            _ => todo!(),
        }
        Ok(())
    }

    let mut logical_plan_splits = Vec::new();
    create_logical_plan_splits(logical_plan, execution_config, &mut logical_plan_splits)?;

    assert!(logical_plan_splits.len() == 1);
    let plans = logical_plan_splits.pop().unwrap();
    let translated_plans = plans
        .into_iter()
        .map(|plan| {
            // Refresh the stats for each logical plan before translating
            let optimizer = OptimizerBuilder::new().enrich_with_stats().build();
            let optimized_plan = optimizer.optimize(plan, |_, _, _, _, _| {})?;
            translate(&optimized_plan)
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(translated_plans)
}
