use std::{
    cmp::{max, min, Ordering},
    collections::HashMap,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_scan_info::{PhysicalScanInfo, ScanState, SPLIT_AND_MERGE_PASS};
use daft_core::{join::JoinSide, prelude::*};
use daft_dsl::{
    estimated_selectivity,
    expr::{
        bound_col,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
    functions::agg::merge_mean,
    is_partition_compatible,
    join::normalize_join_keys,
    lit, resolved_col, AggExpr, ApproxPercentileParams, Expr, ExprRef, SketchType,
};
use daft_functions::numeric::sqrt;
use daft_functions_list::{count_distinct, distinct};
use daft_logical_plan::{
    logical_plan::LogicalPlan,
    ops::{
        ActorPoolProject as LogicalActorPoolProject, Aggregate as LogicalAggregate,
        Distinct as LogicalDistinct, Explode as LogicalExplode, Filter as LogicalFilter,
        Join as LogicalJoin, Limit as LogicalLimit,
        MonotonicallyIncreasingId as LogicalMonotonicallyIncreasingId, Pivot as LogicalPivot,
        Project as LogicalProject, Repartition as LogicalRepartition, Sample as LogicalSample,
        Sink as LogicalSink, Sort as LogicalSort, Source, TopN as LogicalTopN,
        Unpivot as LogicalUnpivot,
    },
    partitioning::{
        ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
    },
    sink_info::{OutputFileInfo, SinkInfo},
    source_info::{PlaceHolderInfo, SourceInfo},
};
use indexmap::IndexSet;

use crate::{ops::*, PhysicalPlan, PhysicalPlanRef};

pub(super) fn translate_single_logical_node(
    logical_plan: &LogicalPlan,
    physical_children: &mut Vec<PhysicalPlanRef>,
    cfg: &DaftExecutionConfig,
) -> DaftResult<PhysicalPlanRef> {
    let physical_plan = match logical_plan {
        LogicalPlan::Source(Source { source_info, .. }) => match source_info.as_ref() {
            SourceInfo::Physical(PhysicalScanInfo {
                pushdowns,
                scan_state,
                source_schema,
                ..
            }) => {
                let scan_tasks = {
                    match scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    }
                };

                if scan_tasks.is_empty() {
                    let clustering_spec =
                        Arc::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)));

                    Ok(PhysicalPlan::EmptyScan(EmptyScan::new(
                        source_schema.clone(),
                        clustering_spec,
                    ))
                    .arced())
                } else {
                    // Perform scan task splitting and merging.
                    let scan_tasks = if let Some(split_and_merge_pass) = SPLIT_AND_MERGE_PASS.get()
                    {
                        split_and_merge_pass(scan_tasks, pushdowns, cfg)?
                    } else {
                        scan_tasks
                    };

                    let clustering_spec = Arc::new(ClusteringSpec::Unknown(
                        UnknownClusteringConfig::new(scan_tasks.len()),
                    ));

                    Ok(
                        PhysicalPlan::TabularScan(TabularScan::new(scan_tasks, clustering_spec))
                            .arced(),
                    )
                }
            }
            SourceInfo::InMemory(mem_info) => {
                let clustering_spec = mem_info.clustering_spec.clone().unwrap_or_else(|| {
                    ClusteringSpec::Unknown(UnknownClusteringConfig::new(mem_info.num_partitions))
                        .into()
                });

                let scan = PhysicalPlan::InMemoryScan(InMemoryScan::new(
                    mem_info.source_schema.clone(),
                    mem_info.clone(),
                    clustering_spec,
                ))
                .arced();
                Ok(scan)
            }
            SourceInfo::PlaceHolder(PlaceHolderInfo { .. }) => {
                panic!("Placeholder should not get to translation. This should have been optimized away");
            }
        },
        LogicalPlan::Shard(_) => {
            return Err(DaftError::InternalError(
                "Sharding should have been folded into a source".to_string(),
            ));
        }
        LogicalPlan::Project(LogicalProject { projection, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(
                PhysicalPlan::Project(Project::try_new(input_physical, projection.clone())?)
                    .arced(),
            )
        }
        LogicalPlan::ActorPoolProject(LogicalActorPoolProject { projection, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(PhysicalPlan::ActorPoolProject(ActorPoolProject::try_new(
                input_physical,
                projection.clone(),
            )?)
            .arced())
        }
        LogicalPlan::Filter(LogicalFilter {
            predicate, input, ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let estimated_selectivity = estimated_selectivity(predicate, &input.schema());
            Ok(PhysicalPlan::Filter(Filter::new(
                input_physical,
                predicate.clone(),
                estimated_selectivity,
            ))
            .arced())
        }
        LogicalPlan::Limit(LogicalLimit { limit, eager, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(
                PhysicalPlan::Limit(Limit::new(input_physical, *limit, *eager, num_partitions))
                    .arced(),
            )
        }
        LogicalPlan::TopN(LogicalTopN {
            sort_by,
            descending,
            nulls_first,
            limit,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(PhysicalPlan::TopN(TopN::new(
                input_physical,
                sort_by.clone(),
                descending.clone(),
                nulls_first.clone(),
                *limit,
                num_partitions,
            ))
            .arced())
        }
        LogicalPlan::Explode(LogicalExplode { to_explode, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(
                PhysicalPlan::Explode(Explode::try_new(input_physical, to_explode.clone())?)
                    .arced(),
            )
        }
        LogicalPlan::Unpivot(LogicalUnpivot {
            ids,
            values,
            variable_name,
            value_name,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");

            Ok(PhysicalPlan::Unpivot(Unpivot::new(
                input_physical,
                ids.clone(),
                values.clone(),
                variable_name,
                value_name,
            ))
            .arced())
        }
        LogicalPlan::Sort(LogicalSort {
            sort_by,
            descending,
            nulls_first,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(PhysicalPlan::Sort(Sort::new(
                input_physical,
                sort_by.clone(),
                descending.clone(),
                nulls_first.clone(),
                num_partitions,
            ))
            .arced())
        }
        LogicalPlan::Repartition(LogicalRepartition {
            repartition_spec, ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let input_clustering_spec = input_physical.clustering_spec();
            let input_num_partitions = input_clustering_spec.num_partitions();
            let clustering_spec = repartition_spec.to_clustering_spec(input_num_partitions);
            let num_partitions = clustering_spec.num_partitions();
            // Drop the repartition if the output of the repartition would yield the same partitioning as the input.
            if (input_num_partitions == 1 && num_partitions == 1)
                // Simple split/coalesce repartition to the same # of partitions is a no-op, no matter the upstream partitioning scheme.
                || (num_partitions == input_num_partitions && matches!(clustering_spec, ClusteringSpec::Unknown(_)))
                // Repartitioning to the same partition spec as the input is always a no-op.
                || (&clustering_spec == input_clustering_spec.as_ref())
            {
                Ok(input_physical)
            } else {
                let repartitioned_plan = match clustering_spec {
                    ClusteringSpec::Unknown(_) => {
                        match num_partitions.cmp(&input_num_partitions) {
                            Ordering::Equal => {
                                // # of output partitions == # of input partitions; this should have already short-circuited with
                                // a repartition drop above.
                                unreachable!("Simple repartitioning with same # of output partitions as the input; this should have been dropped.")
                            }
                            _ => PhysicalPlan::ShuffleExchange(
                                ShuffleExchangeFactory::new(input_physical)
                                    .get_split_or_coalesce(num_partitions),
                            ),
                        }
                    }
                    ClusteringSpec::Random(_) => PhysicalPlan::ShuffleExchange(
                        ShuffleExchangeFactory::new(input_physical)
                            .get_random_partitioning(num_partitions, Some(cfg))?,
                    ),
                    ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => {
                        PhysicalPlan::ShuffleExchange(
                            ShuffleExchangeFactory::new(input_physical).get_hash_partitioning(
                                by,
                                num_partitions,
                                Some(cfg),
                            )?,
                        )
                    }
                    ClusteringSpec::Range(_) => {
                        unreachable!("Repartitioning by range is not supported")
                    }
                };
                Ok(repartitioned_plan.arced())
            }
        }
        LogicalPlan::Distinct(LogicalDistinct { input, columns, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let col_exprs = columns.clone().unwrap_or_else(|| {
                input
                    .schema()
                    .names()
                    .iter()
                    .map(|name| daft_dsl::resolved_col(name.clone()))
                    .collect::<Vec<_>>()
            });
            let agg_op = PhysicalPlan::Dedup(Dedup::new(input_physical, col_exprs.clone()));
            let num_partitions = agg_op.clustering_spec().num_partitions();
            if num_partitions > 1 {
                let shuffle_op = PhysicalPlan::ShuffleExchange(
                    ShuffleExchangeFactory::new(agg_op.into()).get_hash_partitioning(
                        col_exprs.clone(),
                        num_partitions,
                        Some(cfg),
                    )?,
                );
                Ok(PhysicalPlan::Dedup(Dedup::new(shuffle_op.into(), col_exprs)).arced())
            } else {
                Ok(agg_op.arced())
            }
        }
        LogicalPlan::Sample(LogicalSample {
            fraction,
            with_replacement,
            seed,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(PhysicalPlan::Sample(Sample::new(
                input_physical,
                *fraction,
                *with_replacement,
                *seed,
            ))
            .arced())
        }
        LogicalPlan::Aggregate(LogicalAggregate {
            aggregations,
            groupby,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");

            let num_input_partitions = input_physical.clustering_spec().num_partitions();

            let aggregations = aggregations
                .iter()
                .map(extract_agg_expr)
                .collect::<DaftResult<Vec<_>>>()?;

            let result_plan = match num_input_partitions {
                1 => PhysicalPlan::Aggregate(Aggregate::new(
                    input_physical,
                    aggregations,
                    groupby.clone(),
                )),
                _ => {
                    let schema = logical_plan.schema();

                    let (first_stage_aggs, second_stage_aggs, final_exprs) =
                        populate_aggregation_stages(&aggregations, &schema, groupby);

                    let (first_stage_agg, groupby) = if first_stage_aggs.is_empty() {
                        (input_physical, groupby.clone())
                    } else {
                        (
                            PhysicalPlan::Aggregate(Aggregate::new(
                                input_physical,
                                first_stage_aggs.values().cloned().collect(),
                                groupby.clone(),
                            ))
                            .arced(),
                            groupby.iter().map(|e| resolved_col(e.name())).collect(),
                        )
                    };
                    let gather_plan = if groupby.is_empty() {
                        PhysicalPlan::ShuffleExchange(
                            ShuffleExchangeFactory::new(first_stage_agg).get_split_or_coalesce(1),
                        )
                        .into()
                    } else {
                        PhysicalPlan::ShuffleExchange(
                            ShuffleExchangeFactory::new(first_stage_agg).get_hash_partitioning(
                                groupby.clone(),
                                min(
                                    num_input_partitions,
                                    cfg.shuffle_aggregation_default_partitions,
                                ),
                                Some(cfg),
                            )?,
                        )
                        .into()
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan,
                        second_stage_aggs.values().cloned().collect(),
                        groupby,
                    ));

                    PhysicalPlan::Project(Project::try_new(second_stage_agg.into(), final_exprs)?)
                }
            };
            Ok(result_plan.arced())
        }
        LogicalPlan::Pivot(LogicalPivot {
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_input_partitions = input_physical.clustering_spec().num_partitions();

            // NOTE: For the aggregation stages of the pivot operation, we need to group by the group_by column and pivot column together.
            // This is because the resulting pivoted columns correspond to the unique pairs of group_by and pivot column values.
            let group_by_with_pivot = [group_by.clone(), vec![pivot_column.clone()]].concat();
            let aggregations = vec![aggregation.clone()];

            let aggregation_plan = match num_input_partitions {
                1 => PhysicalPlan::Aggregate(Aggregate::new(
                    input_physical,
                    aggregations,
                    group_by_with_pivot,
                )),
                _ => {
                    let schema = logical_plan.schema();

                    let (first_stage_aggs, second_stage_aggs, final_exprs) =
                        populate_aggregation_stages(&aggregations, &schema, &group_by_with_pivot);

                    let first_stage_agg = if first_stage_aggs.is_empty() {
                        input_physical
                    } else {
                        PhysicalPlan::Aggregate(Aggregate::new(
                            input_physical,
                            first_stage_aggs.values().cloned().collect(),
                            group_by_with_pivot.clone(),
                        ))
                        .arced()
                    };
                    let gather_plan = if group_by_with_pivot.is_empty() {
                        PhysicalPlan::ShuffleExchange(
                            ShuffleExchangeFactory::new(first_stage_agg).get_split_or_coalesce(1),
                        )
                        .into()
                    } else {
                        PhysicalPlan::ShuffleExchange(
                            ShuffleExchangeFactory::new(first_stage_agg).get_hash_partitioning(
                                // NOTE: For the shuffle of a pivot operation, we don't include the pivot column for the hashing as we need
                                // to ensure that all rows with the same group_by column values are hashed to the same partition.
                                group_by.clone(),
                                min(
                                    num_input_partitions,
                                    cfg.shuffle_aggregation_default_partitions,
                                ),
                                Some(cfg),
                            )?,
                        )
                        .into()
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan,
                        second_stage_aggs.values().cloned().collect(),
                        group_by_with_pivot,
                    ));

                    PhysicalPlan::Project(Project::try_new(second_stage_agg.into(), final_exprs)?)
                }
            };

            Ok(PhysicalPlan::Pivot(Pivot::new(
                aggregation_plan.into(),
                group_by.clone(),
                pivot_column.clone(),
                value_column.clone(),
                names.clone(),
            ))
            .arced())
        }
        LogicalPlan::Concat(..) => {
            let other_physical = physical_children.pop().expect("requires 1 inputs");
            let input_physical = physical_children.pop().expect("requires 2 inputs");
            Ok(PhysicalPlan::Concat(Concat::new(input_physical, other_physical)).arced())
        }
        LogicalPlan::Join(..) => {
            let (physical_plan, _) = translate_join(physical_children, logical_plan, cfg, false)?;
            Ok(physical_plan)
        }
        LogicalPlan::Sink(LogicalSink {
            schema, sink_info, ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            match sink_info.as_ref() {
                SinkInfo::OutputFileInfo(file_info @ OutputFileInfo { file_format, .. }) => {
                    match file_format {
                        FileFormat::Parquet => {
                            Ok(PhysicalPlan::TabularWriteParquet(TabularWriteParquet::new(
                                schema.clone(),
                                file_info.clone(),
                                input_physical,
                            ))
                            .arced())
                        }
                        FileFormat::Csv => Ok(PhysicalPlan::TabularWriteCsv(TabularWriteCsv::new(
                            schema.clone(),
                            file_info.clone(),
                            input_physical,
                        ))
                        .arced()),
                        FileFormat::Json => {
                            Ok(PhysicalPlan::TabularWriteJson(TabularWriteJson::new(
                                schema.clone(),
                                file_info.clone(),
                                input_physical,
                            ))
                            .arced())
                        }
                        FileFormat::Database => Err(common_error::DaftError::ValueError(
                            "Database sink not yet implemented".to_string(),
                        )),
                        FileFormat::Python => Err(common_error::DaftError::ValueError(
                            "Cannot write to PythonFunction file format".to_string(),
                        )),
                        FileFormat::Warc => Err(common_error::DaftError::ValueError(
                            "Warc sink not yet implemented".to_string(),
                        )),
                    }
                }
                #[cfg(feature = "python")]
                SinkInfo::CatalogInfo(catalog_info) => {
                    use daft_logical_plan::sink_info::CatalogType;
                    match &catalog_info.catalog {
                        CatalogType::Iceberg(iceberg_info) => Ok(PhysicalPlan::IcebergWrite(
                            IcebergWrite::new(schema.clone(), iceberg_info.clone(), input_physical),
                        )
                        .arced()),
                        CatalogType::DeltaLake(deltalake_info) => {
                            Ok(PhysicalPlan::DeltaLakeWrite(DeltaLakeWrite::new(
                                schema.clone(),
                                deltalake_info.clone(),
                                input_physical,
                            ))
                            .arced())
                        }
                        CatalogType::Lance(lance_info) => Ok(PhysicalPlan::LanceWrite(
                            LanceWrite::new(schema.clone(), lance_info.clone(), input_physical),
                        )
                        .arced()),
                    }
                }
                #[cfg(feature = "python")]
                SinkInfo::DataSinkInfo(data_sink_info) => Ok(PhysicalPlan::DataSink(
                    DataSink::new(schema.clone(), data_sink_info.clone(), input_physical),
                )
                .arced()),
            }
        }
        LogicalPlan::MonotonicallyIncreasingId(LogicalMonotonicallyIncreasingId {
            column_name,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(
                PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId::new(
                    input_physical,
                    column_name,
                ))
                .arced(),
            )
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
        LogicalPlan::Window(_window) => Err(DaftError::NotImplemented(
            "Window functions are currently only supported on the native runner.".to_string(),
        )),
    }?;
    // TODO(desmond): We can't perform this check for now because ScanTasks currently provide
    // different size estimations depending on when the approximation is computed. Once we fix
    // this, we can add back in the assertion here.
    // debug_assert!(logical_plan.get_stats().approx_stats == physical_plan.approximate_stats());
    Ok(physical_plan)
}

pub fn adaptively_translate_single_logical_node(
    logical_plan: &LogicalPlan,
    physical_children: &mut Vec<PhysicalPlanRef>,
    cfg: &DaftExecutionConfig,
) -> DaftResult<(PhysicalPlanRef, Option<LogicalPlan>)> {
    match logical_plan {
        LogicalPlan::Join(..) => translate_join(physical_children, logical_plan, cfg, true),
        _ => {
            let physical_plan =
                translate_single_logical_node(logical_plan, physical_children, cfg)?;
            let boundary_placeholder = None;
            Ok((physical_plan, boundary_placeholder))
        }
    }
}

pub fn extract_agg_expr(expr: &ExprRef) -> DaftResult<AggExpr> {
    match expr.as_ref() {
        Expr::Agg(agg_expr) => Ok(agg_expr.clone()),
        Expr::Alias(e, name) => extract_agg_expr(e).map(|agg_expr| {
            // reorder expressions so that alias goes before agg
            match agg_expr {
                AggExpr::Count(e, count_mode) => {
                    AggExpr::Count(Expr::Alias(e, name.clone()).into(), count_mode)
                }
                AggExpr::CountDistinct(e) => {
                    AggExpr::CountDistinct(Expr::Alias(e, name.clone()).into())
                },
                AggExpr::Sum(e) => AggExpr::Sum(Expr::Alias(e, name.clone()).into()),
                AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: e,
                    percentiles,
                    force_list_output,
                }) => AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: Expr::Alias(e, name.clone()).into(),
                    percentiles,
                    force_list_output,
                }),
                AggExpr::ApproxCountDistinct(e) => {
                    AggExpr::ApproxCountDistinct(Expr::Alias(e, name.clone()).into())
                }
                AggExpr::ApproxSketch(e, sketch_type) => {
                    AggExpr::ApproxSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::MergeSketch(e, sketch_type) => {
                    AggExpr::MergeSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::Mean(e) => AggExpr::Mean(Expr::Alias(e, name.clone()).into()),
                AggExpr::Stddev(e) => AggExpr::Stddev(Expr::Alias(e, name.clone()).into()),
                AggExpr::Min(e) => AggExpr::Min(Expr::Alias(e, name.clone()).into()),
                AggExpr::Max(e) => AggExpr::Max(Expr::Alias(e, name.clone()).into()),
                AggExpr::BoolAnd(e) => AggExpr::BoolAnd(Expr::Alias(e, name.clone()).into()),
                AggExpr::BoolOr(e) => AggExpr::BoolOr(Expr::Alias(e, name.clone()).into()),
                AggExpr::AnyValue(e, ignore_nulls) => {
                    AggExpr::AnyValue(Expr::Alias(e, name.clone()).into(), ignore_nulls)
                }
                AggExpr::List(e) => AggExpr::List(Expr::Alias(e, name.clone()).into()),
                AggExpr::Set(e) => AggExpr::Set(Expr::Alias(e, name.clone()).into()),
                AggExpr::Concat(e) => AggExpr::Concat(Expr::Alias(e, name.clone()).into()),
                AggExpr::Skew(e) => AggExpr::Skew(Expr::Alias(e, name.clone()).into()),
                AggExpr::MapGroups { func, inputs } => AggExpr::MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        _ => Err(DaftError::InternalError(
            format!(
                "Expected non-agg expressions in aggregation to be factored out before plan translation. Got: {:?}",
                expr
            )
        )),
    }
}

pub fn populate_aggregation_stages_bound(
    aggregations: &[BoundAggExpr],
    schema: &Schema,
    group_by: &[BoundExpr],
) -> DaftResult<(Vec<BoundAggExpr>, Vec<BoundAggExpr>, Vec<BoundExpr>)> {
    let (
        (first_stage_aggs, _first_stage_schema),
        (second_stage_aggs, _second_stage_schema),
        final_exprs,
    ) = populate_aggregation_stages_bound_with_schema(aggregations, schema, group_by)?;

    Ok((first_stage_aggs, second_stage_aggs, final_exprs))
}

#[allow(clippy::type_complexity)]
pub fn populate_aggregation_stages_bound_with_schema(
    aggregations: &[BoundAggExpr],
    schema: &Schema,
    group_by: &[BoundExpr],
) -> DaftResult<(
    (Vec<BoundAggExpr>, Schema),
    (Vec<BoundAggExpr>, Schema),
    Vec<BoundExpr>,
)> {
    let mut first_stage_aggs = IndexSet::new();
    let mut second_stage_aggs = IndexSet::new();

    let group_by_fields = group_by
        .iter()
        .map(|expr| expr.inner().to_field(schema))
        .collect::<DaftResult<Vec<_>>>()?;
    let mut first_stage_schema = Schema::new(group_by_fields);
    let mut second_stage_schema = first_stage_schema.clone();

    let mut final_exprs = group_by
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let field = e.inner().to_field(schema)?;

            Ok(BoundExpr::new_unchecked(bound_col(i, field)))
        })
        .collect::<DaftResult<Vec<_>>>()?;

    /// Add an agg expr to the specified stage, returning a column expression to reference it.
    ///
    /// If an equivalent expr is already in the stage, simply refer to that expr.
    fn add_to_stage(
        stage: &mut IndexSet<BoundAggExpr>,
        input_schema: &Schema,
        output_schema: &mut Schema,
        groupby_count: usize,
        expr: AggExpr,
    ) -> DaftResult<ExprRef> {
        let (index, is_new) = stage.insert_full(BoundAggExpr::new_unchecked(expr.clone()));
        let index = index + groupby_count;

        if is_new {
            let field = expr.to_field(input_schema)?;
            output_schema.append(field);
        }

        Ok(bound_col(index, output_schema[index].clone()))
    }

    // using macros here instead of closures because borrow checker complains
    // about simultaneous reference + mutable reference
    macro_rules! first_stage {
        ($expr:expr) => {
            add_to_stage(
                &mut first_stage_aggs,
                schema,
                &mut first_stage_schema,
                group_by.len(),
                $expr,
            )?
        };
    }

    macro_rules! second_stage {
        ($expr:expr) => {
            add_to_stage(
                &mut second_stage_aggs,
                &first_stage_schema,
                &mut second_stage_schema,
                group_by.len(),
                $expr,
            )?
        };
    }

    for agg_expr in aggregations {
        let output_field = agg_expr.as_ref().to_field(schema)?;
        let output_name = output_field.name.as_str();

        let mut final_stage = |expr: ExprRef| {
            final_exprs.push(BoundExpr::new_unchecked(expr.alias(output_name)));
        };

        match agg_expr.as_ref() {
            AggExpr::Count(expr, count_mode) => {
                let count_col = first_stage!(AggExpr::Count(expr.clone(), *count_mode));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));
                final_stage(global_count_col);
            }
            AggExpr::CountDistinct(expr) => {
                let set_agg_col = first_stage!(AggExpr::Set(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(set_agg_col));
                final_stage(count_distinct(concat_col));
            }
            AggExpr::Sum(expr) => {
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                final_stage(global_sum_col);
            }
            AggExpr::ApproxPercentile(ApproxPercentileParams {
                child,
                percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(child.clone(), SketchType::DDSketch));
                let merge_sketch_col = second_stage!(AggExpr::MergeSketch(
                    approx_sketch_col,
                    SketchType::DDSketch
                ));
                final_stage(merge_sketch_col.sketch_percentile(&percentiles, *force_list_output));
            }
            AggExpr::ApproxCountDistinct(expr) => {
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(expr.clone(), SketchType::HyperLogLog));
                let merge_sketch_col = second_stage!(AggExpr::MergeSketch(
                    approx_sketch_col,
                    SketchType::HyperLogLog
                ));
                final_stage(merge_sketch_col);
            }
            AggExpr::Mean(expr) => {
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let count_col = first_stage!(AggExpr::Count(expr.clone(), CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                final_stage(merge_mean(global_sum_col, global_count_col));
            }
            AggExpr::Stddev(expr) => {
                // The stddev calculation we're performing here is:
                // stddev(X) = sqrt(E(X^2) - E(X)^2)
                // where X is the sub_expr.
                //
                // First stage, we compute `sum(X^2)`, `sum(X)` and `count(X)`.
                // Second stage, we `global_sqsum := sum(sum(X^2))`, `global_sum := sum(sum(X))` and `global_count := sum(count(X))` in order to get the global versions of the first stage.
                // In the final projection, we then compute `sqrt((global_sqsum / global_count) - (global_sum / global_count) ^ 2)`.

                // This is a workaround since we have different code paths for single stage and two stage aggregations.
                // Currently all Std Dev types will be computed using floats.
                let expr = expr.clone().cast(&DataType::Float64);

                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let sq_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let count_col = first_stage!(AggExpr::Count(expr, CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_sq_sum_col = second_stage!(AggExpr::Sum(sq_sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                let left = global_sq_sum_col.div(global_count_col.clone());
                let right = global_sum_col.div(global_count_col);
                let right = right.clone().mul(right);
                let result = sqrt::sqrt(left.sub(right));

                final_stage(result);
            }
            AggExpr::Min(expr) => {
                let min_col = first_stage!(AggExpr::Min(expr.clone()));
                let global_min_col = second_stage!(AggExpr::Min(min_col));
                final_stage(global_min_col);
            }
            AggExpr::Max(expr) => {
                let max_col = first_stage!(AggExpr::Max(expr.clone()));
                let global_max_col = second_stage!(AggExpr::Max(max_col));
                final_stage(global_max_col);
            }
            AggExpr::BoolAnd(expr) => {
                let bool_and_col = first_stage!(AggExpr::BoolAnd(expr.clone()));
                let global_bool_and_col = second_stage!(AggExpr::BoolAnd(bool_and_col.clone()));
                final_stage(global_bool_and_col);
            }
            AggExpr::BoolOr(expr) => {
                let bool_or_col = first_stage!(AggExpr::BoolOr(expr.clone()));
                let global_bool_or_col = second_stage!(AggExpr::BoolOr(bool_or_col.clone()));
                final_stage(global_bool_or_col);
            }
            AggExpr::AnyValue(expr, ignore_nulls) => {
                let any_col = first_stage!(AggExpr::AnyValue(expr.clone(), *ignore_nulls));
                let global_any_col = second_stage!(AggExpr::AnyValue(any_col, *ignore_nulls));
                final_stage(global_any_col);
            }
            AggExpr::List(expr) => {
                let list_col = first_stage!(AggExpr::List(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(list_col));
                final_stage(concat_col);
            }
            AggExpr::Set(expr) => {
                let set_col = first_stage!(AggExpr::Set(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(set_col));
                final_stage(distinct(concat_col));
            }
            AggExpr::Concat(expr) => {
                let concat_col = first_stage!(AggExpr::Concat(expr.clone()));
                let global_concat_col = second_stage!(AggExpr::Concat(concat_col));
                final_stage(global_concat_col);
            }
            AggExpr::Skew(expr) => {
                // See https://github.com/duckdb/duckdb/blob/93fda3591f4298414fa362c59219c09e03f718ab/extension/core_functions/aggregate/distributive/skew.cpp#L16
                // Not exactly the same since they normalize by N - 1
                let expr = expr.clone().cast(&DataType::Float64);

                // Global count, sum, squared_sum, and cubed_sum are required for final expr

                // First stage: count, sum, squared_sum, cubed_sum
                let count_col = first_stage!(AggExpr::Count(expr.clone(), CountMode::Valid));
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let squared_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let cubed_sum_col =
                    first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone()).mul(expr)));

                // Second stage: sum(count), sum(sum), sum(squared_sum), sum(cubed_sum)
                let global_count_col = second_stage!(AggExpr::Sum(count_col));
                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_squared_sum_col = second_stage!(AggExpr::Sum(squared_sum_col));
                let global_cubed_sum_col = second_stage!(AggExpr::Sum(cubed_sum_col));

                // Final projection: Given
                // - sum(count) = N
                // - sum(sum) = S
                // - sum(squared_sum) = S2
                // - sum(cubed_sum) = S3
                //         S3 - 3 * S2 * S / N + 2 * S^3 / N^2
                // Skew = -------------------------------------
                //          N * ((S2 - S^2 / N) / N) ^ (3/2)

                let n = global_count_col;
                let s = global_sum_col;
                let s2 = global_squared_sum_col;
                let s3 = global_cubed_sum_col;

                let denom_base = s2
                    .clone()
                    .sub(s.clone().mul(s.clone()).div(n.clone()))
                    .div(n.clone());
                let denom = sqrt::sqrt(denom_base.clone().mul(denom_base.clone()).mul(denom_base));

                let numerator = s3.sub(lit(3).mul(s2).mul(s.clone()).div(n.clone())).add(
                    lit(2)
                        .mul(s.clone())
                        .mul(s.clone())
                        .mul(s)
                        .div(n.clone())
                        .div(n.clone()),
                );

                let result = numerator.div(denom).div(n);
                final_stage(result);
            }
            AggExpr::MapGroups { func, inputs } => {
                // No first stage aggregation for MapGroups, do all the work in the second stage.
                let map_groups_col = second_stage!(AggExpr::MapGroups {
                    func: func.clone(),
                    inputs: inputs.clone()
                });
                final_stage(map_groups_col);
            }
            // Only necessary for Flotilla
            AggExpr::ApproxSketch(expr, sketch_type) => {
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(expr.clone(), *sketch_type));
                let merged_sketch_col =
                    second_stage!(AggExpr::MergeSketch(approx_sketch_col, *sketch_type));
                final_stage(merged_sketch_col);
            }
            AggExpr::MergeSketch(expr, sketch_type) => {
                // Merging is commutative and associative, so just keep doing it
                let merge_sketch_col =
                    first_stage!(AggExpr::MergeSketch(expr.clone(), *sketch_type));
                let merged_sketch_col =
                    second_stage!(AggExpr::MergeSketch(merge_sketch_col, *sketch_type));
                final_stage(merged_sketch_col);
            }
        }
    }

    Ok((
        (first_stage_aggs.into_iter().collect(), first_stage_schema),
        (second_stage_aggs.into_iter().collect(), second_stage_schema),
        final_exprs,
    ))
}

/// Given a list of aggregation expressions, return the aggregation expressions to apply in the first and second stages,
/// as well as the final expressions to project.
#[allow(clippy::type_complexity)]
pub fn populate_aggregation_stages(
    aggregations: &[daft_dsl::AggExpr],
    schema: &SchemaRef,
    group_by: &[ExprRef],
) -> (
    HashMap<Arc<str>, daft_dsl::AggExpr>,
    HashMap<Arc<str>, daft_dsl::AggExpr>,
    Vec<ExprRef>,
) {
    // Aggregations to apply in the first and second stages.
    // Semantic column name -> AggExpr
    let mut first_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
    let mut second_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
    // Project the aggregation results to their final output names
    let mut final_exprs: Vec<ExprRef> = group_by.iter().map(|e| resolved_col(e.name())).collect();

    fn add_to_stage<F>(
        f: F,
        expr: ExprRef,
        schema: &Schema,
        stage: &mut HashMap<Arc<str>, AggExpr>,
    ) -> Arc<str>
    where
        F: Fn(ExprRef) -> AggExpr,
    {
        let id = f(expr.clone()).semantic_id(schema).id;
        let agg_expr = f(expr.alias(id.clone()));
        stage.insert(id.clone(), agg_expr);
        id
    }

    for agg_expr in aggregations {
        let output_name = agg_expr.name();
        match agg_expr {
            AggExpr::Count(e, mode) => {
                let count_id = agg_expr.semantic_id(schema).id;
                let sum_of_count_id = AggExpr::Sum(resolved_col(count_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(count_id.clone())
                    .or_insert(AggExpr::Count(e.alias(count_id.clone()).clone(), *mode));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(AggExpr::Sum(
                        resolved_col(count_id.clone()).alias(sum_of_count_id.clone()),
                    ));
                final_exprs.push(resolved_col(sum_of_count_id.clone()).alias(output_name));
            }
            AggExpr::CountDistinct(sub_expr) => {
                // First stage
                let list_agg_id = add_to_stage(
                    AggExpr::Set,
                    sub_expr.clone(),
                    schema,
                    &mut first_stage_aggs,
                );

                // Second stage
                let list_concat_id = add_to_stage(
                    AggExpr::Concat,
                    resolved_col(list_agg_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );

                // Final projection
                let result =
                    count_distinct(resolved_col(list_concat_id.clone())).alias(output_name);
                final_exprs.push(result);
            }
            AggExpr::Sum(e) => {
                let sum_id = agg_expr.semantic_id(schema).id;
                let sum_of_sum_id = AggExpr::Sum(resolved_col(sum_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(sum_id.clone())
                    .or_insert(AggExpr::Sum(e.alias(sum_id.clone()).clone()));
                second_stage_aggs
                    .entry(sum_of_sum_id.clone())
                    .or_insert(AggExpr::Sum(
                        resolved_col(sum_id.clone()).alias(sum_of_sum_id.clone()),
                    ));
                final_exprs.push(resolved_col(sum_of_sum_id.clone()).alias(output_name));
            }
            AggExpr::Mean(e) => {
                let sum_id = AggExpr::Sum(e.clone()).semantic_id(schema).id;
                let count_id = AggExpr::Count(e.clone(), CountMode::Valid)
                    .semantic_id(schema)
                    .id;
                let sum_of_sum_id = AggExpr::Sum(resolved_col(sum_id.clone()))
                    .semantic_id(schema)
                    .id;
                let sum_of_count_id = AggExpr::Sum(resolved_col(count_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(sum_id.clone())
                    .or_insert(AggExpr::Sum(e.alias(sum_id.clone()).clone()));
                first_stage_aggs
                    .entry(count_id.clone())
                    .or_insert(AggExpr::Count(
                        e.alias(count_id.clone()).clone(),
                        CountMode::Valid,
                    ));
                second_stage_aggs
                    .entry(sum_of_sum_id.clone())
                    .or_insert(AggExpr::Sum(
                        resolved_col(sum_id.clone()).alias(sum_of_sum_id.clone()),
                    ));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(AggExpr::Sum(
                        resolved_col(count_id.clone()).alias(sum_of_count_id.clone()),
                    ));
                final_exprs.push(
                    merge_mean(
                        resolved_col(sum_of_sum_id.clone()),
                        resolved_col(sum_of_count_id.clone()),
                    )
                    .alias(output_name),
                );
            }
            AggExpr::Stddev(sub_expr) => {
                // The stddev calculation we're performing here is:
                // stddev(X) = sqrt(E(X^2) - E(X)^2)
                // where X is the sub_expr.
                //
                // First stage, we compute `sum(X^2)`, `sum(X)` and `count(X)`.
                // Second stage, we `global_sqsum := sum(sum(X^2))`, `global_sum := sum(sum(X))` and `global_count := sum(count(X))` in order to get the global versions of the first stage.
                // In the final projection, we then compute `sqrt((global_sqsum / global_count) - (global_sum / global_count) ^ 2)`.

                // This is a workaround since we have different code paths for single stage and two stage aggregations.
                // Currently all Std Dev types will be computed using floats.
                let sub_expr = sub_expr.clone().cast(&DataType::Float64);
                // first stage aggregation
                let sum_id = add_to_stage(
                    AggExpr::Sum,
                    sub_expr.clone(),
                    schema,
                    &mut first_stage_aggs,
                );
                let sq_sum_id = add_to_stage(
                    |sub_expr| AggExpr::Sum(sub_expr.clone().mul(sub_expr)),
                    sub_expr.clone(),
                    schema,
                    &mut first_stage_aggs,
                );
                let count_id = add_to_stage(
                    |sub_expr| AggExpr::Count(sub_expr, CountMode::Valid),
                    sub_expr.clone(),
                    schema,
                    &mut first_stage_aggs,
                );

                // second stage aggregation
                let global_sum_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(sum_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );
                let global_sq_sum_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(sq_sum_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );
                let global_count_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(count_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );

                // final projection
                let g_sq_sum = resolved_col(global_sq_sum_id);
                let g_sum = resolved_col(global_sum_id);
                let g_count = resolved_col(global_count_id);
                let left = g_sq_sum.div(g_count.clone());
                let right = g_sum.div(g_count);
                let right = right.clone().mul(right);
                let result = sqrt::sqrt(left.sub(right)).alias(output_name);

                final_exprs.push(result);
            }
            AggExpr::Min(e) => {
                let min_id = agg_expr.semantic_id(schema).id;
                let min_of_min_id = AggExpr::Min(resolved_col(min_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(min_id.clone())
                    .or_insert(AggExpr::Min(e.alias(min_id.clone()).clone()));
                second_stage_aggs
                    .entry(min_of_min_id.clone())
                    .or_insert(AggExpr::Min(
                        resolved_col(min_id.clone()).alias(min_of_min_id.clone()),
                    ));
                final_exprs.push(resolved_col(min_of_min_id.clone()).alias(output_name));
            }
            AggExpr::Max(e) => {
                let max_id = agg_expr.semantic_id(schema).id;
                let max_of_max_id = AggExpr::Max(resolved_col(max_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(max_id.clone())
                    .or_insert(AggExpr::Max(e.alias(max_id.clone()).clone()));
                second_stage_aggs
                    .entry(max_of_max_id.clone())
                    .or_insert(AggExpr::Max(
                        resolved_col(max_id.clone()).alias(max_of_max_id.clone()),
                    ));
                final_exprs.push(resolved_col(max_of_max_id.clone()).alias(output_name));
            }
            AggExpr::BoolAnd(e) => {
                // First stage
                let bool_and_id =
                    add_to_stage(AggExpr::BoolAnd, e.clone(), schema, &mut first_stage_aggs);

                // Second stage
                let bool_of_bool_and_id = add_to_stage(
                    AggExpr::BoolAnd,
                    resolved_col(bool_and_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );

                // Final projection
                final_exprs.push(resolved_col(bool_of_bool_and_id.clone()).alias(output_name));
            }
            AggExpr::BoolOr(e) => {
                // First stage
                let bool_or_id =
                    add_to_stage(AggExpr::BoolOr, e.clone(), schema, &mut first_stage_aggs);

                // Second stage
                let bool_of_bool_or_id = add_to_stage(
                    AggExpr::BoolOr,
                    resolved_col(bool_or_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );

                // Final projection
                final_exprs.push(resolved_col(bool_of_bool_or_id.clone()).alias(output_name));
            }
            AggExpr::AnyValue(e, ignore_nulls) => {
                let any_id = agg_expr.semantic_id(schema).id;
                let any_of_any_id = AggExpr::AnyValue(resolved_col(any_id.clone()), *ignore_nulls)
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(any_id.clone())
                    .or_insert(AggExpr::AnyValue(
                        e.alias(any_id.clone()).clone(),
                        *ignore_nulls,
                    ));
                second_stage_aggs
                    .entry(any_of_any_id.clone())
                    .or_insert(AggExpr::AnyValue(
                        resolved_col(any_id.clone()).alias(any_of_any_id.clone()),
                        *ignore_nulls,
                    ));
                final_exprs.push(resolved_col(any_of_any_id.clone()).alias(output_name));
            }
            AggExpr::List(e) => {
                let list_id = agg_expr.semantic_id(schema).id;
                let concat_of_list_id = AggExpr::Concat(resolved_col(list_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(list_id.clone())
                    .or_insert(AggExpr::List(e.alias(list_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_list_id.clone())
                    .or_insert(AggExpr::Concat(
                        resolved_col(list_id.clone()).alias(concat_of_list_id.clone()),
                    ));
                final_exprs.push(resolved_col(concat_of_list_id.clone()).alias(output_name));
            }
            AggExpr::Set(e) => {
                let list_agg_id =
                    add_to_stage(AggExpr::Set, e.clone(), schema, &mut first_stage_aggs);
                let list_concat_id = add_to_stage(
                    AggExpr::Concat,
                    resolved_col(list_agg_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );
                let result = distinct(resolved_col(list_concat_id.clone())).alias(output_name);
                final_exprs.push(result);
            }
            AggExpr::Concat(e) => {
                let concat_id = agg_expr.semantic_id(schema).id;
                let concat_of_concat_id = AggExpr::Concat(resolved_col(concat_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(concat_id.clone())
                    .or_insert(AggExpr::Concat(e.alias(concat_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_concat_id.clone())
                    .or_insert(AggExpr::Concat(
                        resolved_col(concat_id.clone()).alias(concat_of_concat_id.clone()),
                    ));
                final_exprs.push(resolved_col(concat_of_concat_id.clone()).alias(output_name));
            }
            AggExpr::Skew(se) => {
                // See https://github.com/duckdb/duckdb/blob/93fda3591f4298414fa362c59219c09e03f718ab/extension/core_functions/aggregate/distributive/skew.cpp#L16
                // Not exactly the same since they normalize by N - 1
                let se = se.clone().cast(&DataType::Float64);

                // Global count, sum, squared_sum, and cubed_sum are required for final expr

                // First stage: count, sum, squared_sum, cubed_sum
                let count_id = add_to_stage(
                    |se| AggExpr::Count(se, CountMode::Valid),
                    se.clone(),
                    schema,
                    &mut first_stage_aggs,
                );
                let sum_id = add_to_stage(AggExpr::Sum, se.clone(), schema, &mut first_stage_aggs);
                let squared_sum_id = add_to_stage(
                    |se| AggExpr::Sum(se.clone().mul(se)),
                    se.clone(),
                    schema,
                    &mut first_stage_aggs,
                );
                let cubed_sum_id = add_to_stage(
                    |se| AggExpr::Sum(se.clone().mul(se.clone()).mul(se)),
                    se.clone(),
                    schema,
                    &mut first_stage_aggs,
                );

                // Second stage: sum(count), sum(sum), sum(squared_sum), sum(cubed_sum)
                let sum_count_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(count_id),
                    schema,
                    &mut second_stage_aggs,
                );
                let sum_sum_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(sum_id),
                    schema,
                    &mut second_stage_aggs,
                );
                let sum_squared_sum_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(squared_sum_id),
                    schema,
                    &mut second_stage_aggs,
                );
                let sum_cubed_sum_id = add_to_stage(
                    AggExpr::Sum,
                    resolved_col(cubed_sum_id),
                    schema,
                    &mut second_stage_aggs,
                );

                // Final projection: Given
                // - sum(count) = N
                // - sum(sum) = S
                // - sum(squared_sum) = S2
                // - sum(cubed_sum) = S3
                //         S3 - 3 * S2 * S / N + 2 * S^3 / N^2
                // Skew = -------------------------------------
                //          N * ((S2 - S^2 / N) / N) ^ (3/2)

                let n = resolved_col(sum_count_id);
                let s = resolved_col(sum_sum_id);
                let s2 = resolved_col(sum_squared_sum_id);
                let s3 = resolved_col(sum_cubed_sum_id);

                let denom_base = s2
                    .clone()
                    .sub(s.clone().mul(s.clone()).div(n.clone()))
                    .div(n.clone());
                let denom = sqrt::sqrt(denom_base.clone().mul(denom_base.clone()).mul(denom_base));

                let numerator = s3.sub(lit(3).mul(s2).mul(s.clone()).div(n.clone())).add(
                    lit(2)
                        .mul(s.clone())
                        .mul(s.clone())
                        .mul(s)
                        .div(n.clone())
                        .div(n.clone()),
                );

                let result = numerator.div(denom).div(n).alias(output_name);
                final_exprs.push(result);
            }
            AggExpr::MapGroups { func, inputs } => {
                let func_id = agg_expr.semantic_id(schema).id;
                // No first stage aggregation for MapGroups, do all the work in the second stage.
                second_stage_aggs
                    .entry(func_id.clone())
                    .or_insert(AggExpr::MapGroups {
                        func: func.clone(),
                        inputs: inputs.clone(),
                    });
                final_exprs.push(resolved_col(output_name));
            }
            &AggExpr::ApproxPercentile(ApproxPercentileParams {
                child: ref e,
                ref percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                let sketch_id = agg_expr.semantic_id(schema).id;
                let approx_id =
                    AggExpr::ApproxSketch(resolved_col(sketch_id.clone()), SketchType::DDSketch)
                        .semantic_id(schema)
                        .id;
                first_stage_aggs
                    .entry(sketch_id.clone())
                    .or_insert(AggExpr::ApproxSketch(
                        e.alias(sketch_id.clone()),
                        SketchType::DDSketch,
                    ));
                second_stage_aggs
                    .entry(approx_id.clone())
                    .or_insert(AggExpr::MergeSketch(
                        resolved_col(sketch_id.clone()).alias(approx_id.clone()),
                        SketchType::DDSketch,
                    ));
                final_exprs.push(
                    resolved_col(approx_id)
                        .sketch_percentile(percentiles.as_slice(), force_list_output)
                        .alias(output_name),
                );
            }
            AggExpr::ApproxCountDistinct(e) => {
                let first_stage_id = agg_expr.semantic_id(schema).id;
                let second_stage_id = AggExpr::MergeSketch(
                    resolved_col(first_stage_id.clone()),
                    SketchType::HyperLogLog,
                )
                .semantic_id(schema)
                .id;
                first_stage_aggs
                    .entry(first_stage_id.clone())
                    .or_insert(AggExpr::ApproxSketch(
                        e.alias(first_stage_id.clone()),
                        SketchType::HyperLogLog,
                    ));
                second_stage_aggs
                    .entry(second_stage_id.clone())
                    .or_insert(AggExpr::MergeSketch(
                        resolved_col(first_stage_id).alias(second_stage_id.clone()),
                        SketchType::HyperLogLog,
                    ));
                final_exprs.push(resolved_col(second_stage_id).alias(output_name));
            }
            AggExpr::ApproxSketch(..) => {
                unimplemented!("User-facing approx_sketch aggregation is not implemented")
            }
            AggExpr::MergeSketch(..) => {
                unimplemented!("User-facing merge_sketch aggregation is not implemented")
            }
        }
    }
    (first_stage_aggs, second_stage_aggs, final_exprs)
}

fn translate_join(
    physical_children: &mut Vec<Arc<PhysicalPlan>>,
    join_plan: &LogicalPlan,
    cfg: &DaftExecutionConfig,
    adaptive: bool,
) -> DaftResult<(PhysicalPlanRef, Option<LogicalPlan>)> {
    let LogicalJoin {
        left,
        right,
        on,
        join_type,
        join_strategy,
        ..
    } = match join_plan {
        LogicalPlan::Join(join_op) => join_op,
        _ => {
            return Err(common_error::DaftError::ValueError(
                "Expected a join plan".to_string(),
            ));
        }
    };

    let mut right_physical = physical_children.pop().expect("requires 1 inputs");
    let mut left_physical = physical_children.pop().expect("requires 2 inputs");

    let (remaining_on, left_on, right_on, null_equals_nulls) = on.split_eq_preds();

    if !remaining_on.is_empty() {
        return Err(DaftError::not_implemented("Execution of non-equality join"));
    }

    let (left_on, right_on) =
        normalize_join_keys(left_on, right_on, left.schema(), right.schema())?;

    let left_clustering_spec = left_physical.clustering_spec();
    let right_clustering_spec = right_physical.clustering_spec();
    let num_partitions = max(
        left_clustering_spec.num_partitions(),
        right_clustering_spec.num_partitions(),
    );

    let is_left_hash_partitioned =
        matches!(left_clustering_spec.as_ref(), ClusteringSpec::Hash(..))
            && is_partition_compatible(&left_clustering_spec.partition_by(), &left_on);
    let is_right_hash_partitioned =
        matches!(right_clustering_spec.as_ref(), ClusteringSpec::Hash(..))
            && is_partition_compatible(&right_clustering_spec.partition_by(), &right_on);

    // Left-side of join is considered to be sort-partitioned on the join key if it is sort-partitioned on a
    // sequence of expressions that has the join key as a prefix.
    let is_left_sort_partitioned =
        if let ClusteringSpec::Range(RangeClusteringConfig { by, descending, .. }) =
            left_clustering_spec.as_ref()
        {
            by.len() >= left_on.len()
                        && by.iter().zip(left_on.iter()).all(|(e1, e2)| e1 == e2)
                        // TODO(Clark): Add support for descending sort orders.
                        && descending.iter().all(|v| !*v)
        } else {
            false
        };
    // Right-side of join is considered to be sort-partitioned on the join key if it is sort-partitioned on a
    // sequence of expressions that has the join key as a prefix.
    let is_right_sort_partitioned =
        if let ClusteringSpec::Range(RangeClusteringConfig { by, descending, .. }) =
            right_clustering_spec.as_ref()
        {
            by.len() >= right_on.len()
                        && by.iter().zip(right_on.iter()).all(|(e1, e2)| e1 == e2)
                        // TODO(Clark): Add support for descending sort orders.
                        && descending.iter().all(|v| !*v)
        } else {
            false
        };
    let left_stats = left_physical.approximate_stats();
    let right_stats = right_physical.approximate_stats();

    // For broadcast joins, ensure that the left side of the join is the smaller side.
    let (smaller_size_bytes, left_is_larger) = if right_stats.size_bytes < left_stats.size_bytes {
        (right_stats.size_bytes, true)
    } else {
        (left_stats.size_bytes, false)
    };
    let is_larger_partitioned = if left_is_larger {
        is_left_hash_partitioned || is_left_sort_partitioned
    } else {
        is_right_hash_partitioned || is_right_sort_partitioned
    };
    let has_null_safe_equals = null_equals_nulls.iter().any(|b| *b);
    let join_strategy = join_strategy.unwrap_or_else(|| {
        if left_on.is_empty() && right_on.is_empty() && join_type == &JoinType::Inner {
            return JoinStrategy::Cross;
        }

        fn keys_are_primitive(on: &[ExprRef], schema: &SchemaRef) -> bool {
            on.iter().all(|expr| {
                let dtype = expr.get_type(schema).unwrap();
                dtype.is_integer()
                    || dtype.is_floating()
                    || matches!(dtype, DataType::Utf8 | DataType::Binary | DataType::Boolean)
            })
        }

        let smaller_side_is_broadcastable = match join_type {
            JoinType::Inner => true,
            JoinType::Left | JoinType::Anti | JoinType::Semi => left_is_larger,
            JoinType::Right => !left_is_larger,
            JoinType::Outer => false,
        };

        // If larger table is not already partitioned on the join key AND the smaller table is under broadcast size threshold AND we are not broadcasting the side we are outer joining by, use broadcast join.
        if !is_larger_partitioned
            && smaller_size_bytes <= cfg.broadcast_join_size_bytes_threshold
            && smaller_side_is_broadcastable
        {
            JoinStrategy::Broadcast
        // Larger side of join is range-partitioned on the join column, so we use a sort-merge join.
        // TODO(Clark): Support non-primitive dtypes for sort-merge join (e.g. temporal types).
        // TODO(Clark): Also do a sort-merge join if a downstream op needs the table to be sorted on the join key.
        // TODO(Clark): Look into defaulting to sort-merge join over hash join under more input partitioning setups.
        // TODO(Kevin): Support sort-merge join for other types of joins.
        // TODO(advancedxy): Rewrite null safe equals to support SMJ
        } else if *join_type == JoinType::Inner
            && keys_are_primitive(&left_on, &left.schema())
            && keys_are_primitive(&right_on, &right.schema())
            && (is_left_sort_partitioned || is_right_sort_partitioned)
            && (!is_larger_partitioned
                || (left_is_larger && is_left_sort_partitioned
                    || !left_is_larger && is_right_sort_partitioned))
            && !has_null_safe_equals
        {
            JoinStrategy::SortMerge
        // Otherwise, use a hash join.
        } else {
            JoinStrategy::Hash
        }
    });

    let left_is_in_memory = matches!(
        left_physical.as_ref(),
        PhysicalPlan::InMemoryScan(InMemoryScan { .. })
    );
    let right_is_in_memory = matches!(
        right_physical.as_ref(),
        PhysicalPlan::InMemoryScan(InMemoryScan { .. })
    );

    match join_strategy {
        JoinStrategy::Broadcast => {
            let is_swapped = match (join_type, left_is_larger) {
                (JoinType::Left, _) => true,
                (JoinType::Right, _) => false,
                (JoinType::Inner, left_is_larger) => left_is_larger,
                (JoinType::Outer, _) => {
                    return Err(common_error::DaftError::ValueError(
                        "Broadcast join does not support outer joins.".to_string(),
                    ));
                }
                (JoinType::Anti, _) => true,
                (JoinType::Semi, _) => true,
            };

            if is_swapped {
                (left_physical, right_physical) = (right_physical, left_physical);
            }

            Ok((
                PhysicalPlan::BroadcastJoin(BroadcastJoin::new(
                    left_physical,
                    right_physical,
                    left_on,
                    right_on,
                    Some(null_equals_nulls),
                    *join_type,
                    is_swapped,
                ))
                .arced(),
                None,
            ))
        }
        // If we are adaptively translating and we decided not to do broadcast join,
        // but our children have not been materialized, we need to materialize them to get stats
        JoinStrategy::SortMerge | JoinStrategy::Hash
            if adaptive && (!left_is_in_memory || !right_is_in_memory) =>
        {
            if left_is_in_memory {
                // if left is in memory it means the right side is not in memory, so we should emit the right side first
                let ph = PlaceHolderInfo::new(right.schema(), right_clustering_spec);
                let new_scan = LogicalPlan::Source(Source::new(
                    right.schema(),
                    SourceInfo::PlaceHolder(ph).into(),
                ));
                let new_join = join_plan.with_new_children(&[left.clone(), new_scan.into()]);
                Ok((right_physical, Some(new_join)))
            } else if right_is_in_memory {
                // if right is in memory it means the left side is not in memory, so we should emit the left side first
                let ph = PlaceHolderInfo::new(left.schema(), left_clustering_spec);
                let new_scan = LogicalPlan::Source(Source::new(
                    left.schema(),
                    SourceInfo::PlaceHolder(ph).into(),
                ));
                let new_join = join_plan.with_new_children(&[new_scan.into(), right.clone()]);
                Ok((left_physical, Some(new_join)))
            } else {
                // if both sides are not in memory, we need to choose one side to emit first
                // bias towards the side with fewer in-memory children
                fn num_in_memory_children(plan: &PhysicalPlan) -> usize {
                    if matches!(plan, PhysicalPlan::InMemoryScan(..)) {
                        1
                    } else {
                        plan.children()
                            .iter()
                            .map(|c| num_in_memory_children(c))
                            .sum()
                    }
                }
                let left_in_memory_children = num_in_memory_children(&left_physical);
                let right_in_memory_children = num_in_memory_children(&right_physical);

                let run_left = match left_in_memory_children.cmp(&right_in_memory_children) {
                    Ordering::Greater => {
                        // left has more in-memory children, so we should emit the left side first
                        true
                    }
                    Ordering::Less => {
                        // right has more in-memory children, so we should emit the left side first
                        false
                    }
                    Ordering::Equal => {
                        // Else pick the smaller side
                        left_stats.size_bytes < right_stats.size_bytes
                    }
                };

                if run_left {
                    let ph = PlaceHolderInfo::new(left.schema(), left_clustering_spec);
                    let new_scan = LogicalPlan::Source(Source::new(
                        left.schema(),
                        SourceInfo::PlaceHolder(ph).into(),
                    ));
                    let new_join = join_plan.with_new_children(&[new_scan.into(), right.clone()]);
                    Ok((left_physical, Some(new_join)))
                } else {
                    let ph = PlaceHolderInfo::new(right.schema(), right_clustering_spec);
                    let new_scan = LogicalPlan::Source(Source::new(
                        right.schema(),
                        SourceInfo::PlaceHolder(ph).into(),
                    ));
                    let new_join = join_plan.with_new_children(&[left.clone(), new_scan.into()]);
                    Ok((right_physical, Some(new_join)))
                }
            }
        }
        JoinStrategy::SortMerge => {
            if *join_type != JoinType::Inner {
                return Err(common_error::DaftError::ValueError(
                    "Sort-merge join currently only supports inner joins".to_string(),
                ));
            }
            if has_null_safe_equals {
                return Err(common_error::DaftError::ValueError(
                    "Sort-merge join does not support null-safe equals yet".to_string(),
                ));
            }

            let needs_presort = if cfg.sort_merge_join_sort_with_aligned_boundaries {
                // Use the special-purpose presorting that ensures join inputs are sorted with aligned
                // boundaries, allowing for a more efficient downstream merge-join (~one-to-one zip).
                !is_left_sort_partitioned || !is_right_sort_partitioned
            } else {
                // Manually insert presorting ops for each side of the join that needs it.
                // Note that these merge-join inputs will most likely not have aligned boundaries, which could
                // result in less efficient merge-joins (~all-to-all broadcast).
                if !is_left_sort_partitioned {
                    left_physical = PhysicalPlan::Sort(Sort::new(
                        left_physical,
                        left_on.clone(),
                        std::iter::repeat_n(false, left_on.len()).collect(),
                        std::iter::repeat_n(false, left_on.len()).collect(),
                        num_partitions,
                    ))
                    .arced();
                }
                if !is_right_sort_partitioned {
                    right_physical = PhysicalPlan::Sort(Sort::new(
                        right_physical,
                        right_on.clone(),
                        std::iter::repeat_n(false, right_on.len()).collect(),
                        std::iter::repeat_n(false, right_on.len()).collect(),
                        num_partitions,
                    ))
                    .arced();
                }
                false
            };
            Ok((
                PhysicalPlan::SortMergeJoin(SortMergeJoin::new(
                    left_physical,
                    right_physical,
                    left_on,
                    right_on,
                    *join_type,
                    num_partitions,
                    left_is_larger,
                    needs_presort,
                ))
                .arced(),
                None,
            ))
        }
        JoinStrategy::Hash => {
            // allow for leniency in the number of partitions
            let num_left_partitions = left_physical.clustering_spec().num_partitions();
            let num_right_partitions = right_physical.clustering_spec().num_partitions();

            let num_partitions = match (
                is_left_hash_partitioned,
                is_right_hash_partitioned,
                num_left_partitions,
                num_right_partitions,
            ) {
                (true, true, a, b) | (false, false, a, b) => max(a, b),
                (_, _, 1, x) | (_, _, x, 1) => x,
                (true, false, a, b)
                    if (a as f64) >= (b as f64) * cfg.hash_join_partition_size_leniency =>
                {
                    a
                }
                (false, true, a, b)
                    if (b as f64) >= (a as f64) * cfg.hash_join_partition_size_leniency =>
                {
                    b
                }
                (_, _, a, b) => max(a, b),
            };
            if num_left_partitions != num_partitions
                || (num_partitions > 1 && !is_left_hash_partitioned)
            {
                left_physical = PhysicalPlan::ShuffleExchange(
                    ShuffleExchangeFactory::new(left_physical).get_hash_partitioning(
                        left_on.clone(),
                        num_partitions,
                        Some(cfg),
                    )?,
                )
                .into();
            }
            if num_right_partitions != num_partitions
                || (num_partitions > 1 && !is_right_hash_partitioned)
            {
                right_physical = PhysicalPlan::ShuffleExchange(
                    ShuffleExchangeFactory::new(right_physical).get_hash_partitioning(
                        right_on.clone(),
                        num_partitions,
                        Some(cfg),
                    )?,
                )
                .into();
            }
            Ok((
                PhysicalPlan::HashJoin(HashJoin::new(
                    left_physical,
                    right_physical,
                    left_on,
                    right_on,
                    Some(null_equals_nulls),
                    *join_type,
                ))
                .arced(),
                None,
            ))
        }
        JoinStrategy::Cross => {
            if *join_type != JoinType::Inner {
                return Err(common_error::DaftError::ValueError(
                    "Cross join is only applicable for inner joins".to_string(),
                ));
            }
            if !left_on.is_empty() || !right_on.is_empty() {
                return Err(common_error::DaftError::ValueError(
                    "Cross join cannot have join keys".to_string(),
                ));
            }

            // choose the larger side to be in the outer loop since the inner side has to be fully materialized
            let outer_loop_side = if left_is_larger {
                JoinSide::Left
            } else {
                JoinSide::Right
            };

            Ok((
                PhysicalPlan::CrossJoin(CrossJoin::new(
                    left_physical,
                    right_physical,
                    outer_loop_side,
                ))
                .arced(),
                None,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, sync::Arc};

    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{lit, resolved_col};
    use daft_logical_plan::LogicalPlanBuilder;

    use super::HashJoin;
    use crate::{
        physical_planner::logical_to_physical,
        test::{dummy_scan_node, dummy_scan_operator},
        PhysicalPlan, PhysicalPlanRef,
    };

    /// Tests that planner drops a simple Repartition (e.g. df.into_partitions()) the child already has the desired number of partitions.
    ///
    /// Repartition-upstream_op -> upstream_op
    #[test]
    fn repartition_dropped_redundant_into_partitions() -> DaftResult<()> {
        let cfg: Arc<DaftExecutionConfig> = DaftExecutionConfig::default().into();
        // dummy_scan_node() will create the default ClusteringSpec, which only has a single partition.
        let builder = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]))
        .into_partitions(10)?
        .filter(resolved_col("a").lt(lit(2)))?;
        assert_eq!(
            logical_to_physical(builder.build(), cfg.clone())?
                .clustering_spec()
                .num_partitions(),
            10
        );
        let logical_plan = builder.into_partitions(10)?.build();
        let physical_plan = logical_to_physical(logical_plan, cfg)?;
        // Check that the last repartition was dropped (the last op should be the filter).
        assert_matches!(physical_plan.as_ref(), PhysicalPlan::Filter(_));
        Ok(())
    }

    /// Tests that planner drops a Repartition if both the Repartition and the child have a single partition.
    ///
    /// Repartition-upstream_op -> upstream_op
    #[test]
    fn repartition_dropped_single_partition() -> DaftResult<()> {
        let cfg: Arc<DaftExecutionConfig> = DaftExecutionConfig::default().into();
        // dummy_scan_node() will create the default ClusteringSpec, which only has a single partition.
        let builder = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]));
        assert_eq!(
            logical_to_physical(builder.build(), cfg.clone())?
                .clustering_spec()
                .num_partitions(),
            1
        );
        let logical_plan = builder
            .hash_repartition(Some(1), vec![resolved_col("a")])?
            .build();
        let physical_plan = logical_to_physical(logical_plan, cfg)?;
        assert_matches!(physical_plan.as_ref(), PhysicalPlan::TabularScan(_));
        Ok(())
    }

    /// Tests that planner drops a Repartition if both the Repartition and the child have the same partition spec.
    ///
    /// Repartition-upstream_op -> upstream_op
    #[test]
    fn repartition_dropped_same_clustering_spec() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]))
        .hash_repartition(Some(10), vec![resolved_col("a")])?
        .filter(resolved_col("a").lt(lit(2)))?
        .hash_repartition(Some(10), vec![resolved_col("a")])?
        .build();
        let physical_plan = logical_to_physical(logical_plan, cfg)?;
        // Check that the last repartition was dropped (the last op should be the filter).
        assert_matches!(physical_plan.as_ref(), PhysicalPlan::Filter(_));
        Ok(())
    }

    /// Tests that planner drops a Repartition if both the Repartition and the upstream Aggregation have the same partition spec.
    ///
    /// Repartition-Aggregation -> Aggregation
    #[test]
    fn repartition_dropped_same_clustering_spec_agg() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]))
        .hash_repartition(Some(10), vec![resolved_col("a")])?
        .aggregate(vec![resolved_col("a").sum()], vec![resolved_col("b")])?
        .hash_repartition(Some(10), vec![resolved_col("b")])?
        .build();
        let physical_plan = logical_to_physical(logical_plan, cfg)?;
        // Check that the last repartition was dropped (the last op should be a projection for a multi-partition aggregation).
        assert_matches!(physical_plan.as_ref(), PhysicalPlan::Project(_));
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum RepartitionOptions {
        Good(usize),
        Bad(usize),
        Reversed(usize),
    }

    impl RepartitionOptions {
        pub fn scale_by(&self, x: usize) -> Self {
            match self {
                Self::Good(v) => Self::Good(v * x),
                Self::Bad(v) => Self::Bad(v * x),
                Self::Reversed(v) => Self::Reversed(v * x),
            }
        }
    }

    fn force_repartition(
        node: LogicalPlanBuilder,
        opts: RepartitionOptions,
    ) -> DaftResult<LogicalPlanBuilder> {
        match opts {
            RepartitionOptions::Good(x) => {
                node.hash_repartition(Some(x), vec![resolved_col("a"), resolved_col("b")])
            }
            RepartitionOptions::Bad(x) => {
                node.hash_repartition(Some(x), vec![resolved_col("a"), resolved_col("c")])
            }
            RepartitionOptions::Reversed(x) => {
                node.hash_repartition(Some(x), vec![resolved_col("b"), resolved_col("a")])
            }
        }
    }

    /// Helper function to get plan for join repartition tests.
    fn get_hash_join_plan(
        cfg: Arc<DaftExecutionConfig>,
        left_partitions: RepartitionOptions,
        right_partitions: RepartitionOptions,
    ) -> DaftResult<Arc<PhysicalPlan>> {
        let join_node = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]));
        let join_node = force_repartition(join_node, right_partitions)?.select(vec![
            resolved_col("a"),
            resolved_col("b"),
            resolved_col("c").alias("dataR"),
        ])?;

        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]));

        let logical_plan = force_repartition(logical_plan, left_partitions)?
            .select(vec![
                resolved_col("a"),
                resolved_col("b"),
                resolved_col("c").alias("dataL"),
            ])?
            .join(
                join_node,
                None,
                vec!["a".to_string(), "b".to_string()],
                JoinType::Inner,
                Some(JoinStrategy::Hash),
                Default::default(),
            )?
            .build();
        logical_to_physical(logical_plan, cfg)
    }

    fn check_physical_matches(
        plan: PhysicalPlanRef,
        left_repartitions: bool,
        right_repartitions: bool,
    ) -> bool {
        match plan.as_ref() {
            PhysicalPlan::HashJoin(HashJoin { left, right, .. }) => {
                let left_works = match (left.as_ref(), left_repartitions) {
                    (PhysicalPlan::ShuffleExchange(_), true) => true,
                    (PhysicalPlan::Project(_), false) => true,
                    _ => false,
                };
                let right_works = match (right.as_ref(), right_repartitions) {
                    (PhysicalPlan::ShuffleExchange(_), true) => true,
                    (PhysicalPlan::Project(_), false) => true,
                    _ => false,
                };
                left_works && right_works
            }
            _ => false,
        }
    }

    /// Tests a variety of settings regarding hash join repartitioning.
    #[test]
    fn repartition_hash_join_tests() -> DaftResult<()> {
        use RepartitionOptions::*;
        let cases = vec![
            (Good(30), Good(30), false, false),
            (Good(30), Good(40), true, false),
            (Good(30), Bad(30), false, true),
            (Good(30), Bad(60), false, true),
            (Good(30), Bad(70), true, true),
            (Reversed(30), Good(30), false, false),
            (Reversed(30), Good(40), true, false),
            (Reversed(30), Bad(30), false, true),
            (Reversed(30), Bad(60), false, true),
            (Reversed(30), Bad(70), true, true),
            (Reversed(30), Reversed(30), false, false),
            (Reversed(30), Reversed(40), true, false),
        ];
        let cfg: Arc<DaftExecutionConfig> = DaftExecutionConfig::default().into();
        for (l_opts, r_opts, l_exp, r_exp) in cases {
            for mult in [1, 10] {
                let plan =
                    get_hash_join_plan(cfg.clone(), l_opts.scale_by(mult), r_opts.scale_by(mult))?;
                assert!(
                    check_physical_matches(plan, l_exp, r_exp),
                    "Failed hash join test on case ({:?}, {:?}, {}, {}) with mult {}",
                    l_opts,
                    r_opts,
                    l_exp,
                    r_exp,
                    mult
                );

                // reversed direction
                let plan =
                    get_hash_join_plan(cfg.clone(), r_opts.scale_by(mult), l_opts.scale_by(mult))?;
                assert!(
                    check_physical_matches(plan, r_exp, l_exp),
                    "Failed hash join test on case ({:?}, {:?}, {}, {}) with mult {}",
                    r_opts,
                    l_opts,
                    r_exp,
                    l_exp,
                    mult
                );
            }
        }
        Ok(())
    }

    /// Tests configuration option for hash join repartition leniency.
    #[test]
    fn repartition_dropped_hash_join_leniency() -> DaftResult<()> {
        let mut cfg = DaftExecutionConfig::default();
        cfg.hash_join_partition_size_leniency = 0.8;
        let cfg = Arc::new(cfg);

        let physical_plan = get_hash_join_plan(
            cfg.clone(),
            RepartitionOptions::Good(20),
            RepartitionOptions::Bad(40),
        )?;
        assert!(check_physical_matches(physical_plan, true, true));

        let physical_plan = get_hash_join_plan(
            cfg.clone(),
            RepartitionOptions::Good(20),
            RepartitionOptions::Bad(25),
        )?;
        assert!(check_physical_matches(physical_plan, false, true));

        let physical_plan = get_hash_join_plan(
            cfg,
            RepartitionOptions::Good(20),
            RepartitionOptions::Bad(26),
        )?;
        assert!(check_physical_matches(physical_plan, true, true));
        Ok(())
    }

    /// Tests that single partitions don't repartition.
    #[test]
    fn hash_join_single_partition_tests() -> DaftResult<()> {
        use RepartitionOptions::*;
        let cases = vec![
            (Good(1), Good(1), false, false),
            (Good(1), Bad(1), false, false),
            (Good(1), Reversed(1), false, false),
            (Bad(1), Bad(1), false, false),
            (Bad(1), Reversed(1), false, false),
        ];
        let cfg: Arc<DaftExecutionConfig> = DaftExecutionConfig::default().into();
        for (l_opts, r_opts, l_exp, r_exp) in cases {
            let plan = get_hash_join_plan(cfg.clone(), l_opts, r_opts)?;
            assert!(
                check_physical_matches(plan, l_exp, r_exp),
                "Failed single partition hash join test on case ({:?}, {:?}, {}, {})",
                l_opts,
                r_opts,
                l_exp,
                r_exp
            );

            // reversed direction
            let plan = get_hash_join_plan(cfg.clone(), r_opts, l_opts)?;
            assert!(
                check_physical_matches(plan, r_exp, l_exp),
                "Failed single partition hash join test on case ({:?}, {:?}, {}, {})",
                r_opts,
                l_opts,
                r_exp,
                l_exp
            );
        }
        Ok(())
    }
}
