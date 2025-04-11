use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_scan_info::ScanState;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{JoinType, LogicalPlan, LogicalPlanRef, SourceInfo};
use daft_scan::ScanTaskRef;

pub fn translate_single_logical_node(
    plan: &LogicalPlanRef,
    inputs: &mut Vec<ScanTaskRef>,
) -> DaftResult<LocalPhysicalPlanRef> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
                    unimplemented!("InMemory scan not supported on distributed swordfish yet")
                }
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    if scan_tasks.is_empty() {
                        unimplemented!("Empty scan not supported on distributed swordfish yet");
                    } else {
                        for scan_task in scan_tasks.iter() {
                            inputs.push(scan_task.clone().as_any_arc().downcast().unwrap());
                        }
                        Ok(LocalPhysicalPlan::stream_scan(
                            source.output_schema.clone(),
                            source.stats_state.clone(),
                        ))
                    }
                }
                SourceInfo::PlaceHolder(_) => {
                    panic!("We should not encounter a PlaceHolder during translation")
                }
            }
        }
        LogicalPlan::Filter(filter) => {
            let input = translate_single_logical_node(&filter.input, inputs)?;
            Ok(LocalPhysicalPlan::filter(
                input,
                filter.predicate.clone(),
                filter.stats_state.clone(),
            ))
        }
        LogicalPlan::Limit(limit) => {
            let input = translate_single_logical_node(&limit.input, inputs)?;
            Ok(LocalPhysicalPlan::limit(
                input,
                limit.limit,
                limit.stats_state.clone(),
            ))
        }
        LogicalPlan::Project(project) => {
            let input = translate_single_logical_node(&project.input, inputs)?;
            Ok(LocalPhysicalPlan::project(
                input,
                project.projection.clone(),
                project.projected_schema.clone(),
                project.stats_state.clone(),
            ))
        }
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            let input = translate_single_logical_node(&actor_pool_project.input, inputs)?;
            Ok(LocalPhysicalPlan::actor_pool_project(
                input,
                actor_pool_project.projection.clone(),
                actor_pool_project.projected_schema.clone(),
                actor_pool_project.stats_state.clone(),
            ))
        }
        LogicalPlan::Sample(sample) => {
            let input = translate_single_logical_node(&sample.input, inputs)?;
            Ok(LocalPhysicalPlan::sample(
                input,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                sample.stats_state.clone(),
            ))
        }
        LogicalPlan::Distinct(distinct) => {
            unimplemented!("Distinct not supported on distributed swordfish yet");
        }
        LogicalPlan::Aggregate(aggregate) => {
            unimplemented!("Aggregate not supported on distributed swordfish yet");
        }
        LogicalPlan::Window(window) => {
            unimplemented!("Window not supported on distributed swordfish yet");
        }
        LogicalPlan::Unpivot(unpivot) => {
            let input = translate_single_logical_node(&unpivot.input, inputs)?;
            Ok(LocalPhysicalPlan::unpivot(
                input,
                unpivot.ids.clone(),
                unpivot.values.clone(),
                unpivot.variable_name.clone(),
                unpivot.value_name.clone(),
                unpivot.output_schema.clone(),
                unpivot.stats_state.clone(),
            ))
        }
        LogicalPlan::Pivot(pivot) => {
            unimplemented!("Pivot not supported on distributed swordfish yet");
        }
        LogicalPlan::Sort(sort) => {
            unimplemented!("Sort not supported on distributed swordfish yet");
        }
        LogicalPlan::Join(join) => {
            unimplemented!("Join not supported on distributed swordfish yet");
        }
        LogicalPlan::Concat(concat) => {
            unimplemented!("Concat not supported on distributed swordfish yet");
        }
        LogicalPlan::Repartition(repartition) => {
            unimplemented!("Repartition not supported on distributed swordfish yet");
        }
        LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
            unimplemented!("MonotonicallyIncreasingId not supported on distributed swordfish yet");
        }
        LogicalPlan::Sink(sink) => {
            use daft_logical_plan::SinkInfo;
            let input = translate_single_logical_node(&sink.input, inputs)?;
            let data_schema = input.schema().clone();
            match sink.sink_info.as_ref() {
                SinkInfo::OutputFileInfo(info) => Ok(LocalPhysicalPlan::physical_write(
                    input,
                    data_schema,
                    sink.schema.clone(),
                    info.clone(),
                    sink.stats_state.clone(),
                )),
                #[cfg(feature = "python")]
                SinkInfo::CatalogInfo(info) => match &info.catalog {
                    daft_logical_plan::CatalogType::DeltaLake(..)
                    | daft_logical_plan::CatalogType::Iceberg(..) => {
                        Ok(LocalPhysicalPlan::catalog_write(
                            input,
                            info.catalog.clone(),
                            data_schema,
                            sink.schema.clone(),
                            sink.stats_state.clone(),
                        ))
                    }
                    daft_logical_plan::CatalogType::Lance(info) => {
                        Ok(LocalPhysicalPlan::lance_write(
                            input,
                            info.clone(),
                            data_schema,
                            sink.schema.clone(),
                            sink.stats_state.clone(),
                        ))
                    }
                },
            }
        }
        LogicalPlan::Explode(explode) => {
            let input = translate_single_logical_node(&explode.input, inputs)?;
            Ok(LocalPhysicalPlan::explode(
                input,
                explode.to_explode.clone(),
                explode.exploded_schema.clone(),
                explode.stats_state.clone(),
            ))
        }
        LogicalPlan::Intersect(_) => Err(DaftError::InternalError(
            "Intersect should already be optimized away".to_string(),
        )),
        LogicalPlan::Union(_) => Err(DaftError::InternalError(
            "Union should already be optimized away".to_string(),
        )),
        LogicalPlan::SubqueryAlias(_) => Err(DaftError::InternalError(
            "Alias should already be optimized away".to_string(),
        )),
    }
}
