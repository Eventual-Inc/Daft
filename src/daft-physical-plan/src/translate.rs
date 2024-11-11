use common_error::{DaftError, DaftResult};
use daft_core::join::JoinStrategy;
use daft_dsl::ExprRef;
use daft_plan::{JoinType, LogicalPlan, LogicalPlanRef, SourceInfo, stats::ApproxStats};

use crate::local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};

pub fn translate(plan: &LogicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> {
    // Statistics of operators change during logical plan optimization (due to moving operators around
    // the query tree + pushdowns), but once we translate the logical plan to a physical plan the stats
    // stop changing. So we can materialize the final stats at translation time and cache these in the
    // physical plan operators.
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
                    let approx_stats = ApproxStats {
                        lower_bound_rows: info.num_rows,
                        upper_bound_rows: Some(info.num_rows),
                        lower_bound_bytes: info.size_bytes,
                        upper_bound_bytes: Some(info.size_bytes),
                    };
                    Ok(LocalPhysicalPlan::in_memory_scan(
                        info.clone(),
                        approx_stats,
                    ))
                },
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks_iter = info.scan_op.0.to_scan_tasks(info.pushdowns.clone())?;
                    let scan_tasks = scan_tasks_iter.collect::<DaftResult<Vec<_>>>()?;
                    if scan_tasks.is_empty() {
                        Ok(LocalPhysicalPlan::empty_scan(
                            source.output_schema.clone(),
                            ApproxStats::empty(),
                        ))
                    } else {
                        Ok(LocalPhysicalPlan::physical_scan(
                            scan_tasks,
                            source.output_schema.clone(),
                            ApproxStats::from(&scan_tasks),
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
            let input_stats = input.approximate_stats();
            let approx_stats = ApproxStats {
                lower_bound_rows: 0,
                upper_bound_rows: input_stats.upper_bound_rows,
                lower_bound_bytes: 0,
                upper_bound_bytes: input_stats.upper_bound_bytes,
            };
            Ok(LocalPhysicalPlan::filter(
                input,
                filter.predicate.clone(),
                approx_stats,
            ))
        }
        LogicalPlan::Limit(limit) => {
            let input = translate(&limit.input)?;
            Ok(LocalPhysicalPlan::limit(input, limit.limit, approx_stats))
        }
        LogicalPlan::Project(project) => {
            let input = translate(&project.input)?;
            Ok(LocalPhysicalPlan::project(
                input,
                project.projection.clone(),
                project.projected_schema.clone(),
                approx_stats,
            ))
        }
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            let input = translate(&actor_pool_project.input)?;
            Ok(LocalPhysicalPlan::actor_pool_project(
                input,
                actor_pool_project.projection.clone(),
                actor_pool_project.projected_schema.clone(),
                approx_stats,
            ))
        }
        LogicalPlan::Sample(sample) => {
            let input = translate(&sample.input)?;
            Ok(LocalPhysicalPlan::sample(
                input,
                sample.fraction,
                sample.with_replacement,
                sample.seed,
                approx_stats,
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = translate(&aggregate.input)?;
            if aggregate.groupby.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.output_schema.clone(),
                    approx_stats,
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    aggregate.aggregations.clone(),
                    aggregate.groupby.clone(),
                    aggregate.output_schema.clone(),
                    approx_stats,
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
                approx_stats,
            ))
        }
        LogicalPlan::Pivot(pivot) => {
            let input = translate(&pivot.input)?;
<<<<<<< Updated upstream
=======
            let groupby_with_pivot = pivot
                .group_by
                .iter()
                .chain(std::iter::once(&pivot.pivot_column))
                .cloned()
                .collect::<Vec<_>>();
            let aggregate_fields = groupby_with_pivot
                .iter()
                .map(|expr| expr.to_field(input.schema()))
                .chain(std::iter::once(pivot.aggregation.to_field(input.schema())))
                .collect::<DaftResult<Vec<_>>>()?;
            let aggregate_schema = Schema::new(aggregate_fields)?;
            let aggregate = LocalPhysicalPlan::hash_aggregate(
                input,
                vec![pivot.aggregation.clone(); 1],
                groupby_with_pivot,
                aggregate_schema.into(),
                approx_stats.clone(), // TODO(desmond): this ain't right.
            );
>>>>>>> Stashed changes
            Ok(LocalPhysicalPlan::pivot(
                input,
                pivot.group_by.clone(),
                pivot.pivot_column.clone(),
                pivot.value_column.clone(),
                pivot.aggregation.clone(),
                pivot.names.clone(),
                pivot.output_schema.clone(),
                approx_stats,
            ))
        }
        LogicalPlan::Sort(sort) => {
            let input = translate(&sort.input)?;
            Ok(LocalPhysicalPlan::sort(
                input,
                sort.sort_by.clone(),
                sort.descending.clone(),
                approx_stats,
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
                join.join_type,
                join.output_schema.clone(),
                approx_stats,
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
                approx_stats,
            ))
        }
        LogicalPlan::Concat(concat) => {
            let input = translate(&concat.input)?;
            let other = translate(&concat.other)?;
            Ok(LocalPhysicalPlan::concat(input, other, approx_stats))
        }
        LogicalPlan::Repartition(repartition) => {
            log::warn!("Repartition Not supported for Local Executor!; This will be a No-Op");
            translate(&repartition.input)
        }
        LogicalPlan::Sink(sink) => {
            use daft_plan::SinkInfo;
            let input = translate(&sink.input)?;
            let data_schema = input.schema().clone();
            match sink.sink_info.as_ref() {
                SinkInfo::OutputFileInfo(info) => Ok(LocalPhysicalPlan::physical_write(
                    input,
                    data_schema,
                    sink.schema.clone(),
                    info.clone(),
                    approx_stats,
                )),
                #[cfg(feature = "python")]
                SinkInfo::CatalogInfo(_) => todo!("CatalogInfo not yet implemented"),
            }
        }
        LogicalPlan::Explode(explode) => {
            let input = translate(&explode.input)?;
            Ok(LocalPhysicalPlan::explode(
                input,
                explode.to_explode.clone(),
                explode.exploded_schema.clone(),
                approx_stats,
            ))
        }
        _ => todo!("{} not yet implemented", plan.name()),
    }
}
