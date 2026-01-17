use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_scan_info::ScanState;
use daft_core::join::JoinStrategy;
use daft_dsl::{
    expr::{
        agg::extract_agg_expr,
        bound_expr::{BoundAggExpr, BoundExpr, BoundVLLMExpr, BoundWindowExpr},
    },
    join::normalize_join_keys,
    resolved_col, window_to_agg_exprs,
};
use daft_logical_plan::{JoinType, LogicalPlan, LogicalPlanRef, SourceInfo, stats::StatsState};

use super::plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, SamplingMethod};
use crate::{SourceId, SourceIdCounter, UnresolvedInput};

/// Translate a logical plan into a physical plan and extract input specifications.
/// Returns the physical plan and a map of source_id -> UnresolvedInput.
pub fn translate(
    plan: &LogicalPlanRef,
) -> DaftResult<(LocalPhysicalPlanRef, HashMap<SourceId, UnresolvedInput>)> {
    let mut source_counter = SourceIdCounter::default();
    translate_helper(plan, &mut source_counter)
}

fn translate_helper(
    plan: &LogicalPlanRef,
    source_counter: &mut SourceIdCounter,
) -> DaftResult<(LocalPhysicalPlanRef, HashMap<SourceId, UnresolvedInput>)> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => {
            let mut unresolved_inputs = HashMap::new();
            let physical_plan = match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
                    let source_id = source_counter.next();
                    let unresolved_input = UnresolvedInput::InMemory {
                        cache_key: info.cache_key.clone(),
                    };
                    unresolved_inputs.insert(source_id, unresolved_input);

                    LocalPhysicalPlan::in_memory_scan(
                        source_id,
                        source.output_schema.clone(),
                        info.size_bytes,
                        source.stats_state.clone(),
                        LocalNodeContext::default(),
                    )
                }
                SourceInfo::Physical(info) => {
                    // We should be able to pass the ScanOperator into the physical plan directly but we need to figure out the serialization story
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    // Create a physical scan
                    let source_id = source_counter.next();

                    // Create InputSpec for scan tasks
                    let unresolved_input = UnresolvedInput::ScanTask(scan_tasks);
                    unresolved_inputs.insert(source_id, unresolved_input);

                    LocalPhysicalPlan::physical_scan(
                        source_id,
                        info.pushdowns.clone(),
                        source.output_schema.clone(),
                        source.stats_state.clone(),
                        LocalNodeContext::default(),
                    )
                }
                SourceInfo::GlobScan(info) => {
                    // Create a glob scan
                    let source_id = source_counter.next();

                    // Create InputSpec for glob paths
                    let unresolved_input = UnresolvedInput::GlobPaths(info.glob_paths.clone());
                    unresolved_inputs.insert(source_id, unresolved_input);

                    LocalPhysicalPlan::glob_scan(
                        source_id,
                        info.pushdowns.clone(),
                        source.output_schema.clone(),
                        source.stats_state.clone(),
                        info.io_config.clone().map(|c| *c),
                        LocalNodeContext::default(),
                    )
                }
                SourceInfo::PlaceHolder(ph) => {
                    // Placeholders don't have inputs
                    LocalPhysicalPlan::placeholder_scan(
                        ph.source_schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::default(),
                    )
                }
            };
            Ok((physical_plan, unresolved_inputs))
        }
        LogicalPlan::Shard(_) => Err(DaftError::InternalError(
            "Sharding should have been folded into a source".to_string(),
        )),
        LogicalPlan::Filter(filter) => {
            let (input_plan, unresolved_inputs) = translate_helper(&filter.input, source_counter)?;
            let predicate = BoundExpr::try_new(filter.predicate.clone(), input_plan.schema())?;
            Ok((
                LocalPhysicalPlan::filter(
                    input_plan,
                    predicate,
                    filter.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::IntoBatches(into_batches) => {
            let (input_plan, unresolved_inputs) =
                translate_helper(&into_batches.input, source_counter)?;
            Ok((
                LocalPhysicalPlan::into_batches(
                    input_plan,
                    into_batches.batch_size,
                    false,
                    into_batches.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Limit(limit) => {
            let (input_plan, unresolved_inputs) = translate_helper(&limit.input, source_counter)?;
            Ok((
                LocalPhysicalPlan::limit(
                    input_plan,
                    limit.limit,
                    limit.offset,
                    limit.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Project(project) => {
            let (input_plan, unresolved_inputs) = translate_helper(&project.input, source_counter)?;

            let projection = BoundExpr::bind_all(&project.projection, input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::project(
                    input_plan,
                    projection,
                    project.projected_schema.clone(),
                    project.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::UDFProject(udf_project) => {
            let (input_plan, unresolved_inputs) =
                translate_helper(&udf_project.input, source_counter)?;

            let passthrough_columns =
                BoundExpr::bind_all(&udf_project.passthrough_columns, input_plan.schema())?;
            let expr = BoundExpr::try_new(udf_project.expr.clone(), input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::udf_project(
                    input_plan,
                    expr,
                    udf_project.udf_properties.clone(),
                    passthrough_columns,
                    udf_project.projected_schema.clone(),
                    udf_project.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Sample(sample) => {
            let (input_plan, unresolved_inputs) = translate_helper(&sample.input, source_counter)?;
            let sampling_method = if let Some(fraction) = sample.fraction {
                SamplingMethod::Fraction(fraction)
            } else if let Some(size) = sample.size {
                SamplingMethod::Size(size)
            } else {
                return Err(DaftError::ValueError(
                    "Either fraction or size must be specified for sample".to_string(),
                ));
            };
            Ok((
                LocalPhysicalPlan::sample(
                    input_plan,
                    sampling_method,
                    sample.with_replacement,
                    sample.seed,
                    sample.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let (input_plan, unresolved_inputs) =
                translate_helper(&aggregate.input, source_counter)?;

            let aggregations = aggregate
                .aggregations
                .iter()
                .map(|expr| {
                    let agg_expr = extract_agg_expr(expr)?;
                    BoundAggExpr::try_new(agg_expr, input_plan.schema())
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let groupby = BoundExpr::bind_all(&aggregate.groupby, input_plan.schema())?;

            if aggregate.groupby.is_empty() {
                Ok((
                    LocalPhysicalPlan::ungrouped_aggregate(
                        input_plan,
                        aggregations,
                        aggregate.output_schema.clone(),
                        aggregate.stats_state.clone(),
                        LocalNodeContext::default(),
                    ),
                    unresolved_inputs,
                ))
            } else {
                Ok((
                    LocalPhysicalPlan::hash_aggregate(
                        input_plan,
                        aggregations,
                        groupby,
                        aggregate.output_schema.clone(),
                        aggregate.stats_state.clone(),
                        LocalNodeContext::default(),
                    ),
                    unresolved_inputs,
                ))
            }
        }
        LogicalPlan::Window(window) => {
            let (input_plan, unresolved_inputs) = translate_helper(&window.input, source_counter)?;

            let window_functions =
                BoundWindowExpr::bind_all(&window.window_functions, input_plan.schema())?;

            let partition_by =
                BoundExpr::bind_all(&window.window_spec.partition_by, input_plan.schema())?;

            let order_by = BoundExpr::bind_all(&window.window_spec.order_by, input_plan.schema())?;

            let plan = match (
                !partition_by.is_empty(),
                !order_by.is_empty(),
                window.window_spec.frame.is_some(),
            ) {
                (true, false, false) => {
                    let aggregations = window_to_agg_exprs(window_functions)?;
                    LocalPhysicalPlan::window_partition_only(
                        input_plan,
                        partition_by,
                        window.schema.clone(),
                        window.stats_state.clone(),
                        aggregations,
                        window.aliases.clone(),
                        LocalNodeContext::default(),
                    )
                }
                (true, true, false) => LocalPhysicalPlan::window_partition_and_order_by(
                    input_plan,
                    partition_by,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.schema.clone(),
                    window.stats_state.clone(),
                    window_functions,
                    window.aliases.clone(),
                    LocalNodeContext::default(),
                ),
                (true, true, true) => {
                    let aggregations = window_to_agg_exprs(window_functions)?;
                    LocalPhysicalPlan::window_partition_and_dynamic_frame(
                        input_plan,
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
                        LocalNodeContext::default(),
                    )
                }
                (false, true, false) => LocalPhysicalPlan::window_order_by_only(
                    input_plan,
                    order_by,
                    window.window_spec.descending.clone(),
                    window.window_spec.nulls_first.clone(),
                    window.schema.clone(),
                    window.stats_state.clone(),
                    window_functions,
                    window.aliases.clone(),
                    LocalNodeContext::default(),
                ),
                (false, true, true) => {
                    return Err(DaftError::not_implemented(
                        "Window with order by and frame not yet implemented",
                    ));
                }
                _ => {
                    return Err(DaftError::ValueError(
                        "Window requires either partition by or order by".to_string(),
                    ));
                }
            };
            Ok((plan, unresolved_inputs))
        }
        LogicalPlan::Unpivot(unpivot) => {
            let (input_plan, unresolved_inputs) = translate_helper(&unpivot.input, source_counter)?;

            let ids = BoundExpr::bind_all(&unpivot.ids, input_plan.schema())?;
            let values = BoundExpr::bind_all(&unpivot.values, input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::unpivot(
                    input_plan,
                    ids,
                    values,
                    unpivot.variable_name.clone(),
                    unpivot.value_name.clone(),
                    unpivot.output_schema.clone(),
                    unpivot.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Pivot(pivot) => {
            let (input_plan, unresolved_inputs) = translate_helper(&pivot.input, source_counter)?;

            let group_by = BoundExpr::bind_all(&pivot.group_by, input_plan.schema())?;
            let pivot_column = BoundExpr::try_new(pivot.pivot_column.clone(), input_plan.schema())?;
            let value_column = BoundExpr::try_new(pivot.value_column.clone(), input_plan.schema())?;
            let aggregation =
                BoundAggExpr::try_new(pivot.aggregation.clone(), input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::pivot(
                    input_plan,
                    group_by,
                    pivot_column,
                    value_column,
                    aggregation,
                    pivot.names.clone(),
                    true,
                    pivot.output_schema.clone(),
                    pivot.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Sort(sort) => {
            let (input_plan, unresolved_inputs) = translate_helper(&sort.input, source_counter)?;

            let sort_by = BoundExpr::bind_all(&sort.sort_by, input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::sort(
                    input_plan,
                    sort_by,
                    sort.descending.clone(),
                    sort.nulls_first.clone(),
                    sort.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::TopN(top_n) => {
            let (input_plan, unresolved_inputs) = translate_helper(&top_n.input, source_counter)?;

            let sort_by = BoundExpr::bind_all(&top_n.sort_by, input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::top_n(
                    input_plan,
                    sort_by,
                    top_n.descending.clone(),
                    top_n.nulls_first.clone(),
                    top_n.limit,
                    top_n.offset,
                    top_n.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Join(join) => {
            if let Some(strategy) = join.join_strategy {
                match strategy {
                    JoinStrategy::Broadcast => {
                        log::warn!(
                            "Broadcast join is not supported on the native runner, falling back to hash join. Please use the ray runner, daft.set_runner_ray(), if you require broadcast joins."
                        );
                    }
                    JoinStrategy::SortMerge => {
                        log::warn!(
                            "Sort merge join is not supported on the native runner, falling back to hash join."
                        );
                    }
                    _ => {}
                }
            }
            let (left_plan, mut left_unresolved_inputs) =
                translate_helper(&join.left, source_counter)?;
            let (right_plan, right_unresolved_inputs) =
                translate_helper(&join.right, source_counter)?;

            // Merge inputs from both sides
            left_unresolved_inputs.extend(right_unresolved_inputs);

            let (remaining_on, left_on, right_on, null_equals_nulls) = join.on.split_eq_preds();

            if !remaining_on.is_empty() {
                return Err(DaftError::not_implemented("Execution of non-equality join"));
            }

            let (left_on, right_on) =
                normalize_join_keys(left_on, right_on, join.left.schema(), join.right.schema())?;

            if left_on.is_empty() && right_on.is_empty() && join.join_type == JoinType::Inner {
                Ok((
                    LocalPhysicalPlan::cross_join(
                        left_plan,
                        right_plan,
                        join.output_schema.clone(),
                        join.stats_state.clone(),
                        LocalNodeContext::default(),
                    ),
                    left_unresolved_inputs,
                ))
            } else {
                let left_on = BoundExpr::bind_all(&left_on, left_plan.schema())?;
                let right_on = BoundExpr::bind_all(&right_on, right_plan.schema())?;

                Ok((
                    LocalPhysicalPlan::hash_join(
                        left_plan,
                        right_plan,
                        left_on,
                        right_on,
                        None,
                        Some(null_equals_nulls),
                        join.join_type,
                        join.output_schema.clone(),
                        join.stats_state.clone(),
                        LocalNodeContext::default(),
                    ),
                    left_unresolved_inputs,
                ))
            }
        }
        LogicalPlan::Distinct(distinct) => {
            let schema = distinct.input.schema();
            let (input_plan, unresolved_inputs) =
                translate_helper(&distinct.input, source_counter)?;

            let columns = distinct
                .columns
                .clone()
                .unwrap_or_else(|| schema.field_names().map(resolved_col).collect::<Vec<_>>());
            let columns = BoundExpr::bind_all(&columns, &schema)?;

            Ok((
                LocalPhysicalPlan::dedup(
                    input_plan,
                    columns,
                    schema,
                    distinct.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Concat(concat) => {
            let (input_plan, mut unresolved_inputs) =
                translate_helper(&concat.input, source_counter)?;
            let (other_plan, other_unresolved_inputs) =
                translate_helper(&concat.other, source_counter)?;

            // Merge inputs from both sides
            unresolved_inputs.extend(other_unresolved_inputs);

            Ok((
                LocalPhysicalPlan::concat(
                    input_plan,
                    other_plan,
                    concat.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Repartition(repartition) => {
            log::warn!(
                "Repartition not supported on the NativeRunner. This will be a no-op. Please use the RayRunner instead if you need to repartition"
            );
            translate_helper(&repartition.input, source_counter)
        }
        LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
            let (input_plan, unresolved_inputs) =
                translate_helper(&monotonically_increasing_id.input, source_counter)?;
            Ok((
                LocalPhysicalPlan::monotonically_increasing_id(
                    input_plan,
                    monotonically_increasing_id.column_name.clone(),
                    monotonically_increasing_id.starting_offset,
                    monotonically_increasing_id.schema.clone(),
                    monotonically_increasing_id.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Sink(sink) => {
            use daft_logical_plan::SinkInfo;
            let (input_plan, unresolved_inputs) = translate_helper(&sink.input, source_counter)?;
            let data_schema = input_plan.schema().clone();
            let plan = match sink.sink_info.as_ref() {
                SinkInfo::OutputFileInfo(info) => {
                    let bound_info = info.clone().bind(&data_schema)?;
                    let physical_write = LocalPhysicalPlan::physical_write(
                        input_plan,
                        data_schema.clone(),
                        sink.schema.clone(),
                        bound_info.clone(),
                        sink.stats_state.clone(),
                        LocalNodeContext::default(),
                    );

                    LocalPhysicalPlan::commit_write(
                        physical_write,
                        data_schema,
                        sink.schema.clone(),
                        bound_info,
                        sink.stats_state.clone(),
                        LocalNodeContext::default(),
                    )
                }
                #[cfg(feature = "python")]
                SinkInfo::CatalogInfo(info) => match &info.catalog {
                    daft_logical_plan::CatalogType::DeltaLake(..)
                    | daft_logical_plan::CatalogType::Iceberg(..) => {
                        LocalPhysicalPlan::catalog_write(
                            input_plan,
                            info.catalog.clone().bind(&data_schema)?,
                            data_schema,
                            sink.schema.clone(),
                            sink.stats_state.clone(),
                            LocalNodeContext::default(),
                        )
                    }
                    daft_logical_plan::CatalogType::Lance(info) => LocalPhysicalPlan::lance_write(
                        input_plan,
                        info.clone(),
                        data_schema,
                        sink.schema.clone(),
                        sink.stats_state.clone(),
                        LocalNodeContext::default(),
                    ),
                },
                #[cfg(feature = "python")]
                SinkInfo::DataSinkInfo(data_sink_info) => LocalPhysicalPlan::data_sink(
                    input_plan,
                    data_sink_info.clone(),
                    sink.schema.clone(),
                    sink.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
            };
            Ok((plan, unresolved_inputs))
        }
        LogicalPlan::Explode(explode) => {
            let (input_plan, unresolved_inputs) = translate_helper(&explode.input, source_counter)?;

            let to_explode = BoundExpr::bind_all(&explode.to_explode, input_plan.schema())?;

            Ok((
                LocalPhysicalPlan::explode(
                    input_plan,
                    to_explode,
                    explode.index_column.clone(),
                    explode.exploded_schema.clone(),
                    explode.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::VLLMProject(vllm_project) => {
            let (input_plan, unresolved_inputs) =
                translate_helper(&vllm_project.input, source_counter)?;
            let expr = BoundVLLMExpr::try_new(vllm_project.expr.clone(), input_plan.schema())?;
            Ok((
                LocalPhysicalPlan::vllm_project(
                    input_plan,
                    expr,
                    None,
                    vllm_project.output_column_name.clone(),
                    vllm_project.output_schema.clone(),
                    vllm_project.stats_state.clone(),
                    LocalNodeContext::default(),
                ),
                unresolved_inputs,
            ))
        }
        LogicalPlan::Intersect(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Offset(_) => Err(DaftError::InternalError(format!(
            "Logical plan operator {} should already be optimized away",
            plan.name()
        ))),
    }
}
