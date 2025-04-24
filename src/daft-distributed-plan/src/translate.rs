use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, ScanState};
use daft_local_plan::{translate, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::{Filter, Source},
    LogicalPlan, LogicalPlanRef, SourceInfo,
};

pub fn translate_logical_plan_to_local_physical_plans(
    logical_plan: LogicalPlanRef,
    execution_config: Arc<DaftExecutionConfig>,
) -> DaftResult<Vec<LocalPhysicalPlanRef>> {
    fn create_logical_plan_splits(
        logical_plan: LogicalPlanRef,
        execution_config: Arc<DaftExecutionConfig>,
        logical_plan_splits: &mut Vec<Vec<LogicalPlanRef>>,
    ) -> DaftResult<()> {
        match logical_plan.as_ref() {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
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
                                .estimate_in_memory_size_bytes(Some(&execution_config))
                                .unwrap_or(0);
                            if scan_tasks_bytes_so_far > execution_config.scan_tasks_max_size_bytes
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
                            } else {
                                scan_tasks_accumulator.push(scan_task.clone());
                            }
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
            LogicalPlan::Filter(filter) => {
                create_logical_plan_splits(
                    filter.input.clone(),
                    execution_config,
                    logical_plan_splits,
                )?;
                assert!(logical_plan_splits.len() == 1);
                let plans = logical_plan_splits.pop().unwrap();
                let plans_with_filter = plans
                    .into_iter()
                    .map(|plan| logical_plan.with_new_children(&[plan]).into())
                    .collect();
                logical_plan_splits.push(plans_with_filter);
            }
            LogicalPlan::Project(project) => {
                create_logical_plan_splits(
                    project.input.clone(),
                    execution_config,
                    logical_plan_splits,
                )?;
                assert!(logical_plan_splits.len() == 1);
                let plans = logical_plan_splits.pop().unwrap();
                let plans_with_project = plans
                    .into_iter()
                    .map(|plan| logical_plan.with_new_children(&[plan]).into())
                    .collect();
                logical_plan_splits.push(plans_with_project);
            }
            LogicalPlan::Limit(_) => panic!("Limit should have been converted into a program"),
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
            let owned_plan = Arc::unwrap_or_clone(plan);
            let plan = owned_plan.with_materialized_stats();
            translate(&plan)
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(translated_plans)
}
