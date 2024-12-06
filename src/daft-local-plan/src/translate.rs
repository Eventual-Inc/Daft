use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_scan_info::ScanState;
use daft_core::join::JoinStrategy;
use daft_dsl::ExprRef;
use daft_logical_plan::{JoinType, LogicalPlan, LogicalPlanRef, SourceInfo};

use super::plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};

pub fn translate(plan: &LogicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => Ok(LocalPhysicalPlan::in_memory_scan(
                    info.clone(),
                    source.stats_state.clone(),
                )),
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    if scan_tasks.is_empty() {
                        Ok(LocalPhysicalPlan::empty_scan(source.output_schema.clone()))
                    } else {
                        Ok(LocalPhysicalPlan::physical_scan(
                            scan_tasks,
                            info.pushdowns.clone(),
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
            let input = translate(&filter.input)?;
            Ok(LocalPhysicalPlan::filter(
                input,
                filter.predicate.clone(),
                filter.stats_state.clone(),
            ))
        }
        LogicalPlan::Limit(limit) => {
            let input = translate(&limit.input)?;
            Ok(LocalPhysicalPlan::limit(
                input,
                limit.limit,
                limit.stats_state.clone(),
            ))
        }
        LogicalPlan::Project(project) => {
            let input = translate(&project.input)?;
            Ok(LocalPhysicalPlan::project(
                input,
                project.projection.clone(),
                project.projected_schema.clone(),
                project.stats_state.clone(),
            ))
        }
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            let input = translate(&actor_pool_project.input)?;
            Ok(LocalPhysicalPlan::actor_pool_project(
                input,
                actor_pool_project.projection.clone(),
                actor_pool_project.projected_schema.clone(),
                actor_pool_project.stats_state.clone(),
            ))
        }
        LogicalPlan::Sample(sample) => {
            let input = translate(&sample.input)?;
            Ok(LocalPhysicalPlan::sample(
                input,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                sample.stats_state.clone(),
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = translate(&aggregate.input)?;
            if aggregate.groupby.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.output_schema.clone(),
                    aggregate.stats_state.clone(),
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.groupby.clone(),
                    aggregate.output_schema.clone(),
                    aggregate.stats_state.clone(),
                ))
            }
        }
        LogicalPlan::Unpivot(unpivot) => {
            let input = translate(&unpivot.input)?;
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
            let input = translate(&pivot.input)?;
            Ok(LocalPhysicalPlan::pivot(
                input,
                pivot.group_by.clone(),
                pivot.pivot_column.clone(),
                pivot.value_column.clone(),
                pivot.aggregation.clone(),
                pivot.names.clone(),
                pivot.output_schema.clone(),
                pivot.stats_state.clone(),
            ))
        }
        LogicalPlan::Sort(sort) => {
            let input = translate(&sort.input)?;
            Ok(LocalPhysicalPlan::sort(
                input,
                sort.sort_by.clone(),
                sort.descending.clone(),
                sort.nulls_first.clone(),
                sort.stats_state.clone(),
            ))
        }
        LogicalPlan::Join(join) => {
            if join.left_on.is_empty()
                && join.right_on.is_empty()
                && join.join_type == JoinType::Inner
            {
                return Err(DaftError::not_implemented(
                    "Joins without join conditions (cross join) are not supported yet",
                ));
            }
            if join.join_strategy.is_some_and(|x| x != JoinStrategy::Hash) {
                return Err(DaftError::not_implemented(
                    "Only hash join is supported for now",
                ));
            }
            let left = translate(&join.left)?;
            let right = translate(&join.right)?;
            Ok(LocalPhysicalPlan::hash_join(
                left,
                right,
                join.left_on.clone(),
                join.right_on.clone(),
                join.null_equals_nulls.clone(),
                join.join_type,
                join.output_schema.clone(),
                join.stats_state.clone(),
            ))
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let input = translate(&distinct.input)?;
            let col_exprs = input
                .schema()
                .names()
                .iter()
                .map(|name| daft_dsl::col(name.clone()))
                .collect::<Vec<ExprRef>>();
            Ok(LocalPhysicalPlan::hash_aggregate(
                input,
                vec![],
                col_exprs,
                schema,
                distinct.stats_state.clone(),
            ))
        }
        LogicalPlan::Concat(concat) => {
            let input = translate(&concat.input)?;
            let other = translate(&concat.other)?;
            Ok(LocalPhysicalPlan::concat(
                input,
                other,
                concat.stats_state.clone(),
            ))
        }
        LogicalPlan::Repartition(repartition) => {
            log::warn!("Repartition Not supported for Local Executor!; This will be a No-Op");
            translate(&repartition.input)
        }
        LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
            let input = translate(&monotonically_increasing_id.input)?;
            Ok(LocalPhysicalPlan::monotonically_increasing_id(
                input,
                monotonically_increasing_id.column_name.clone(),
                monotonically_increasing_id.schema.clone(),
                monotonically_increasing_id.stats_state.clone(),
            ))
        }
        LogicalPlan::Sink(sink) => {
            use daft_logical_plan::SinkInfo;
            let input = translate(&sink.input)?;
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
            let input = translate(&explode.input)?;
            Ok(LocalPhysicalPlan::explode(
                input,
                explode.to_explode.clone(),
                explode.exploded_schema.clone(),
                explode.stats_state.clone(),
            ))
        }
        _ => todo!("{} not yet implemented", plan.name()),
    }
}
