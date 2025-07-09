use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::WriteMode;
use common_scan_info::ScanState;
use daft_core::join::JoinStrategy;
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr, BoundWindowExpr},
    join::normalize_join_keys,
    resolved_col, WindowExpr,
};
use daft_logical_plan::{stats::StatsState, JoinType, LogicalPlan, LogicalPlanRef, SourceInfo};
use daft_physical_plan::extract_agg_expr;

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
                SourceInfo::PlaceHolder(ph) => Ok(LocalPhysicalPlan::placeholder_scan(
                    ph.source_schema.clone(),
                    StatsState::NotMaterialized,
                )),
            }
        }
        LogicalPlan::Shard(_) => Err(DaftError::InternalError(
            "Sharding should have been folded into a source".to_string(),
        )),
        LogicalPlan::Filter(filter) => {
            let input = translate(&filter.input)?;
            let predicate = BoundExpr::try_new(filter.predicate.clone(), input.schema())?;
            Ok(LocalPhysicalPlan::filter(
                input,
                predicate,
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

            let projection = BoundExpr::bind_all(&project.projection, input.schema())?;

            Ok(LocalPhysicalPlan::project(
                input,
                projection,
                project.projected_schema.clone(),
                project.stats_state.clone(),
            ))
        }
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            let input = translate(&actor_pool_project.input)?;

            let projection = BoundExpr::bind_all(&actor_pool_project.projection, input.schema())?;

            Ok(LocalPhysicalPlan::actor_pool_project(
                input,
                projection,
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

            let aggregations = aggregate
                .aggregations
                .iter()
                .map(|expr| {
                    let agg_expr = extract_agg_expr(expr)?;
                    BoundAggExpr::try_new(agg_expr, input.schema())
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let groupby = BoundExpr::bind_all(&aggregate.groupby, input.schema())?;

            if aggregate.groupby.is_empty() {
                Ok(LocalPhysicalPlan::ungrouped_aggregate(
                    input,
                    aggregations,
                    aggregate.output_schema.clone(),
                    aggregate.stats_state.clone(),
                ))
            } else {
                Ok(LocalPhysicalPlan::hash_aggregate(
                    input,
                    aggregations,
                    groupby,
                    aggregate.output_schema.clone(),
                    aggregate.stats_state.clone(),
                ))
            }
        }
        LogicalPlan::Window(window) => {
            let input = translate(&window.input)?;

            let window_functions =
                BoundWindowExpr::bind_all(&window.window_functions, input.schema())?;

            let partition_by =
                BoundExpr::bind_all(&window.window_spec.partition_by, input.schema())?;

            let order_by = BoundExpr::bind_all(&window.window_spec.order_by, input.schema())?;

            match (
                !partition_by.is_empty(),
                !order_by.is_empty(),
                window.window_spec.frame.is_some(),
            ) {
                (true, false, false) => {
                    let aggregations = window_functions
                        .iter()
                        .map(|w| {
                            if let WindowExpr::Agg(agg_expr) = w.as_ref() {
                                Ok(BoundAggExpr::new_unchecked(agg_expr.clone()))
                            } else {
                                Err(DaftError::TypeError(format!(
                                    "Window function {:?} not implemented in partition-only windows, only aggregation functions are supported",
                                    w
                                )))
                            }
                        })
                        .collect::<DaftResult<Vec<_>>>()?;

                    Ok(LocalPhysicalPlan::window_partition_only(
                        input,
                        partition_by,
                        window.schema.clone(),
                        window.stats_state.clone(),
                        aggregations,
                        window.aliases.clone(),
                    ))
                }
                (true, true, false) => Ok(LocalPhysicalPlan::window_partition_and_order_by(
                    input,
                    partition_by,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.schema.clone(),
                    window.stats_state.clone(),
                    window_functions,
                    window.aliases.clone(),
                )),
                (true, true, true) => {
                    let aggregations = window_functions
                        .iter()
                        .map(|w| {
                            if let WindowExpr::Agg(agg_expr) = w.as_ref() {
                                BoundAggExpr::new_unchecked(agg_expr.clone())
                            } else {
                                panic!("Expected AggExpr")
                            }
                        })
                        .collect::<Vec<_>>();

                    Ok(LocalPhysicalPlan::window_partition_and_dynamic_frame(
                        input,
                        partition_by,
                        order_by,
                        window.window_spec.descending.clone(),
                        window.window_spec.nulls_first.clone(),
                        window.window_spec.frame.clone().unwrap(),
                        window.window_spec.min_periods,
                        window.schema.clone(),
                        window.stats_state.clone(),
                        aggregations,
                        window.aliases.clone(),
                    ))
                }
                (false, true, false) => Ok(LocalPhysicalPlan::window_order_by_only(
                    input,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.schema.clone(),
                    window.stats_state.clone(),
                    window_functions,
                    window.aliases.clone(),
                )),
                (false, true, true) => Err(DaftError::not_implemented(
                    "Window with order by and frame not yet implemented",
                )),
                _ => Err(DaftError::ValueError(
                    "Window requires either partition by or order by".to_string(),
                )),
            }
        }
        LogicalPlan::Unpivot(unpivot) => {
            let input = translate(&unpivot.input)?;

            let ids = BoundExpr::bind_all(&unpivot.ids, input.schema())?;
            let values = BoundExpr::bind_all(&unpivot.values, input.schema())?;

            Ok(LocalPhysicalPlan::unpivot(
                input,
                ids,
                values,
                unpivot.variable_name.clone(),
                unpivot.value_name.clone(),
                unpivot.output_schema.clone(),
                unpivot.stats_state.clone(),
            ))
        }
        LogicalPlan::Pivot(pivot) => {
            let input = translate(&pivot.input)?;

            let group_by = BoundExpr::bind_all(&pivot.group_by, input.schema())?;
            let pivot_column = BoundExpr::try_new(pivot.pivot_column.clone(), input.schema())?;
            let value_column = BoundExpr::try_new(pivot.value_column.clone(), input.schema())?;
            let aggregation = BoundAggExpr::try_new(pivot.aggregation.clone(), input.schema())?;

            Ok(LocalPhysicalPlan::pivot(
                input,
                group_by,
                pivot_column,
                value_column,
                aggregation,
                pivot.names.clone(),
                pivot.output_schema.clone(),
                pivot.stats_state.clone(),
            ))
        }
        LogicalPlan::Sort(sort) => {
            let input = translate(&sort.input)?;

            let sort_by = BoundExpr::bind_all(&sort.sort_by, input.schema())?;

            Ok(LocalPhysicalPlan::sort(
                input,
                sort_by,
                sort.descending.clone(),
                sort.nulls_first.clone(),
                sort.stats_state.clone(),
            ))
        }
        LogicalPlan::TopN(top_n) => {
            let input = translate(&top_n.input)?;

            let sort_by = BoundExpr::bind_all(&top_n.sort_by, input.schema())?;

            Ok(LocalPhysicalPlan::top_n(
                input,
                sort_by,
                top_n.descending.clone(),
                top_n.nulls_first.clone(),
                top_n.limit,
                top_n.stats_state.clone(),
            ))
        }
        LogicalPlan::Join(join) => {
            if join.join_strategy.is_some_and(|x| x != JoinStrategy::Hash) {
                return Err(DaftError::not_implemented(
                    "Only hash join is supported for now",
                ));
            }
            let left = translate(&join.left)?;
            let right = translate(&join.right)?;

            let (remaining_on, left_on, right_on, null_equals_nulls) = join.on.split_eq_preds();

            if !remaining_on.is_empty() {
                return Err(DaftError::not_implemented("Execution of non-equality join"));
            }

            let (left_on, right_on) =
                normalize_join_keys(left_on, right_on, join.left.schema(), join.right.schema())?;

            if left_on.is_empty() && right_on.is_empty() && join.join_type == JoinType::Inner {
                Ok(LocalPhysicalPlan::cross_join(
                    left,
                    right,
                    join.output_schema.clone(),
                    join.stats_state.clone(),
                ))
            } else {
                let left_on = BoundExpr::bind_all(&left_on, left.schema())?;
                let right_on = BoundExpr::bind_all(&right_on, right.schema())?;

                Ok(LocalPhysicalPlan::hash_join(
                    left,
                    right,
                    left_on,
                    right_on,
                    Some(null_equals_nulls),
                    join.join_type,
                    join.output_schema.clone(),
                    join.stats_state.clone(),
                ))
            }
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let input = translate(&distinct.input)?;

            let columns = distinct
                .columns
                .clone()
                .unwrap_or_else(|| schema.field_names().map(resolved_col).collect::<Vec<_>>());
            let columns = BoundExpr::bind_all(&columns, &schema)?;

            Ok(LocalPhysicalPlan::dedup(
                input,
                columns,
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
            log::warn!("Repartition not supported on the NativeRunner. This will be a no-op. Please use the RayRunner instead if you need to repartition");
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
                SinkInfo::OutputFileInfo(info) => {
                    let bound_info = info.clone().bind(&data_schema)?;
                    let physical_write = LocalPhysicalPlan::physical_write(
                        input,
                        data_schema,
                        sink.schema.clone(),
                        bound_info.clone(),
                        sink.stats_state.clone(),
                    );
                    if matches!(
                        info.write_mode,
                        WriteMode::Overwrite | WriteMode::OverwritePartitions
                    ) {
                        Ok(LocalPhysicalPlan::commit_write(
                            physical_write,
                            sink.schema.clone(),
                            bound_info,
                            sink.stats_state.clone(),
                        ))
                    } else {
                        Ok(physical_write)
                    }
                }
                #[cfg(feature = "python")]
                SinkInfo::CatalogInfo(info) => match &info.catalog {
                    daft_logical_plan::CatalogType::DeltaLake(..)
                    | daft_logical_plan::CatalogType::Iceberg(..) => {
                        Ok(LocalPhysicalPlan::catalog_write(
                            input,
                            info.catalog.clone().bind(&data_schema)?,
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
                #[cfg(feature = "python")]
                SinkInfo::DataSinkInfo(data_sink_info) => Ok(LocalPhysicalPlan::data_sink(
                    input,
                    data_sink_info.clone(),
                    sink.schema.clone(),
                    sink.stats_state.clone(),
                )),
            }
        }
        LogicalPlan::Explode(explode) => {
            let input = translate(&explode.input)?;

            let to_explode = BoundExpr::bind_all(&explode.to_explode, input.schema())?;

            Ok(LocalPhysicalPlan::explode(
                input,
                to_explode,
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
