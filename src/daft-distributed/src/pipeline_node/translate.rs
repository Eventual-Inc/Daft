use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use daft_dsl::{join::normalize_join_keys, AggExpr, ExprRef, WindowExpr};
use daft_local_plan::{translate, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    stats::StatsState, JoinStrategy, JoinType, LogicalPlan, LogicalPlanRef, SourceInfo,
};

use super::PipelineInput;

static SOURCE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn get_next_source_id() -> usize {
    SOURCE_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[derive(Clone)]
pub(crate) struct PipelinePlan {
    pub local_plan: LocalPhysicalPlanRef,
    pub input: PipelineInput,
}

#[allow(dead_code)]
pub(crate) fn translate_logical_plan_to_pipeline_plan(
    logical_plan: &LogicalPlanRef,
    execution_config: &DaftExecutionConfig,
    psets: &mut HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<PipelinePlan> {
    // Translate the logical plan to a local plan and a list of inputs
    fn translate_logical_plan_to_local_plan_and_inputs(
        logical_plan: &LogicalPlanRef,
        execution_config: &DaftExecutionConfig,
        psets: &mut HashMap<String, Vec<PartitionRef>>,
        pipeline_input: &mut Option<PipelineInput>,
    ) -> DaftResult<LocalPhysicalPlanRef> {
        match logical_plan.as_ref() {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
                    let partition_refs = psets.get(&info.cache_key).unwrap().clone();
                    assert!(
                        pipeline_input.is_none(),
                        "Pipelines cannot have multiple inputs"
                    );
                    *pipeline_input = Some(PipelineInput::InMemorySource {
                        cache_key: info.cache_key.clone(),
                        partition_refs,
                    });
                    let local_plan = LocalPhysicalPlan::in_memory_scan(
                        info.clone(),
                        daft_logical_plan::stats::StatsState::NotMaterialized,
                    );
                    Ok(local_plan)
                }
                SourceInfo::Physical(info) => {
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    if scan_tasks.is_empty() {
                        Ok(LocalPhysicalPlan::empty_scan(source.output_schema.clone()))
                    } else {
                        assert!(
                            pipeline_input.is_none(),
                            "Pipelines cannot have multiple inputs"
                        );
                        *pipeline_input = Some(PipelineInput::ScanTasks {
                            source_id: get_next_source_id(),
                            pushdowns: info.pushdowns.clone(),
                            scan_tasks,
                        });
                        Ok(LocalPhysicalPlan::placeholder_scan(
                            source.output_schema.clone(),
                            get_next_source_id(),
                            source.stats_state.clone(),
                        ))
                    }
                }
                SourceInfo::PlaceHolder(info) => {
                    assert!(
                        pipeline_input.is_none(),
                        "Pipelines cannot have multiple inputs"
                    );
                    *pipeline_input = Some(PipelineInput::Intermediate);
                    Ok(LocalPhysicalPlan::placeholder_scan(
                        info.source_schema.clone(),
                        get_next_source_id(),
                        StatsState::NotMaterialized,
                    ))
                }
            },
            LogicalPlan::Filter(filter) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &filter.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::filter(
                    input,
                    filter.predicate.clone(),
                    filter.stats_state.clone(),
                ))
            }
            LogicalPlan::Limit(limit) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &limit.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::limit(
                    input,
                    limit.limit,
                    limit.stats_state.clone(),
                ))
            }
            LogicalPlan::Project(project) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &project.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::project(
                    input,
                    project.projection.clone(),
                    project.projected_schema.clone(),
                    project.stats_state.clone(),
                ))
            }
            LogicalPlan::ActorPoolProject(actor_pool_project) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &actor_pool_project.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::actor_pool_project(
                    input,
                    actor_pool_project.projection.clone(),
                    actor_pool_project.projected_schema.clone(),
                    actor_pool_project.stats_state.clone(),
                ))
            }
            LogicalPlan::Sample(sample) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &sample.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::sample(
                    input,
                    sample.fraction,
                    sample.with_replacement,
                    sample.seed,
                    sample.stats_state.clone(),
                ))
            }
            LogicalPlan::Aggregate(aggregate) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &aggregate.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
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
            LogicalPlan::Window(window) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &window.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                match (
                    !window.window_spec.partition_by.is_empty(),
                    !window.window_spec.order_by.is_empty(),
                    window.window_spec.frame.is_some(),
                ) {
                    (true, false, false) => {
                        let aggregations = window
                            .window_functions
                            .iter()
                            .map(|w| {
                                if let WindowExpr::Agg(agg_expr) = w {
                                    Ok(agg_expr.clone())
                                } else {
                                    Err(DaftError::TypeError(format!(
                                        "Window function {:?} not implemented in partition-only windows, only aggregation functions are supported",
                                        w
                                    )))
                                }
                            })
                            .collect::<DaftResult<Vec<AggExpr>>>()?;

                        Ok(LocalPhysicalPlan::window_partition_only(
                            input,
                            window.window_spec.partition_by.clone(),
                            window.schema.clone(),
                            window.stats_state.clone(),
                            aggregations,
                            window.aliases.clone(),
                        ))
                    }
                    (true, true, false) => Ok(LocalPhysicalPlan::window_partition_and_order_by(
                        input,
                        window.window_spec.partition_by.clone(),
                        window.window_spec.order_by.clone(),
                        window.window_spec.descending.clone(),
                        window.schema.clone(),
                        window.stats_state.clone(),
                        window.window_functions.clone(),
                        window.aliases.clone(),
                    )),
                    (true, true, true) => {
                        let aggregations = window
                            .window_functions
                            .iter()
                            .map(|w| {
                                if let WindowExpr::Agg(agg_expr) = w {
                                    agg_expr.clone()
                                } else {
                                    panic!("Expected AggExpr")
                                }
                            })
                            .collect::<Vec<AggExpr>>();

                        Ok(LocalPhysicalPlan::window_partition_and_dynamic_frame(
                            input,
                            window.window_spec.partition_by.clone(),
                            window.window_spec.order_by.clone(),
                            window.window_spec.descending.clone(),
                            window.window_spec.frame.clone().unwrap(),
                            window.window_spec.min_periods,
                            window.schema.clone(),
                            window.stats_state.clone(),
                            aggregations,
                            window.aliases.clone(),
                        ))
                    }
                    _ => Err(DaftError::not_implemented(
                        "Window with order by or frame not yet implemented",
                    )),
                }
            }
            LogicalPlan::Unpivot(unpivot) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &unpivot.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
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
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &pivot.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
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
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &sort.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::sort(
                    input,
                    sort.sort_by.clone(),
                    sort.descending.clone(),
                    sort.nulls_first.clone(),
                    sort.stats_state.clone(),
                ))
            }
            LogicalPlan::Join(join) => {
                if join.join_strategy.is_some_and(|x| x != JoinStrategy::Hash) {
                    return Err(DaftError::not_implemented(
                        "Only hash join is supported for now",
                    ));
                }
                let left = translate_logical_plan_to_local_plan_and_inputs(
                    &join.left,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                let right = translate_logical_plan_to_local_plan_and_inputs(
                    &join.right,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;

                let (remaining_on, left_on, right_on, null_equals_nulls) = join.on.split_eq_preds();

                if !remaining_on.is_empty() {
                    return Err(DaftError::not_implemented("Execution of non-equality join"));
                }

                let (left_on, right_on) = normalize_join_keys(
                    left_on,
                    right_on,
                    join.left.schema(),
                    join.right.schema(),
                )?;

                if left_on.is_empty() && right_on.is_empty() && join.join_type == JoinType::Inner {
                    Ok(LocalPhysicalPlan::cross_join(
                        left,
                        right,
                        join.output_schema.clone(),
                        join.stats_state.clone(),
                    ))
                } else {
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
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &distinct.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                let col_exprs = input
                    .schema()
                    .names()
                    .iter()
                    .map(|name| daft_dsl::resolved_col(name.clone()))
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
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &concat.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                let other = translate_logical_plan_to_local_plan_and_inputs(
                    &concat.other,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::concat(
                    input,
                    other,
                    concat.stats_state.clone(),
                ))
            }
            LogicalPlan::Repartition(repartition) => {
                translate_logical_plan_to_local_plan_and_inputs(
                    &repartition.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )
            }
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &monotonically_increasing_id.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
                Ok(LocalPhysicalPlan::monotonically_increasing_id(
                    input,
                    monotonically_increasing_id.column_name.clone(),
                    monotonically_increasing_id.schema.clone(),
                    monotonically_increasing_id.stats_state.clone(),
                ))
            }
            LogicalPlan::Sink(sink) => {
                use daft_logical_plan::SinkInfo;
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &sink.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
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
                let input = translate_logical_plan_to_local_plan_and_inputs(
                    &explode.input,
                    execution_config,
                    psets,
                    pipeline_input,
                )?;
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

    let mut input = None;
    let local_plan = translate_logical_plan_to_local_plan_and_inputs(
        logical_plan,
        execution_config,
        psets,
        &mut input,
    )?;
    Ok(PipelinePlan {
        local_plan,
        input: input.expect("Pipeline input should be set"),
    })
}
