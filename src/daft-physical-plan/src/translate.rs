use common_error::DaftResult;
use daft_plan::{LogicalPlan, LogicalPlanRef, SourceInfo};

use crate::local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};

pub fn translate(plan: &LogicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => Ok(LocalPhysicalPlan::in_memory_scan(info.clone())),
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks_iter = info.scan_op.0.to_scan_tasks(info.pushdowns.clone())?;
                    let scan_tasks = scan_tasks_iter.collect::<DaftResult<Vec<_>>>()?;
                    Ok(LocalPhysicalPlan::physical_scan(
                        scan_tasks,
                        info.source_schema.clone(),
                    ))
                }
                SourceInfo::PlaceHolder(_) => {
                    panic!("We should not encounter a PlaceHolder during translation")
                }
            }
        }
        LogicalPlan::Filter(filter) => {
            let input = translate(&filter.input)?;
            Ok(LocalPhysicalPlan::filter(input, filter.predicate.clone()))
        }
        LogicalPlan::Limit(limit) => {
            let input = translate(&limit.input)?;
            Ok(LocalPhysicalPlan::limit(input, limit.limit))
        }
        LogicalPlan::Project(project) => {
            let input = translate(&project.input)?;
            Ok(LocalPhysicalPlan::project(
                input,
                project.projection.clone(),
                project.resource_request.clone(),
                project.projected_schema.clone(),
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = translate(&aggregate.input)?;
            if aggregate.groupby.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.output_schema.clone(),
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.groupby.clone(),
                    aggregate.output_schema.clone(),
                ))
            }
        }
        _ => todo!("{} not yet implemented", plan.name()),
    }
}
