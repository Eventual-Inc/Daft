use std::cmp::Ordering;
use std::sync::Arc;
use std::{
    cmp::{max, min},
    collections::HashMap,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_core::count_mode::CountMode;
use daft_core::DataType;
use daft_dsl::Expr;
use daft_scan::ScanExternalInfo;

use crate::logical_ops::{
    Aggregate as LogicalAggregate, Concat as LogicalConcat, Distinct as LogicalDistinct,
    Explode as LogicalExplode, Filter as LogicalFilter, Join as LogicalJoin, Limit as LogicalLimit,
    MonotonicallyIncreasingId as LogicalMonotonicallyIncreasingId, Project as LogicalProject,
    Repartition as LogicalRepartition, Sample as LogicalSample, Sink as LogicalSink,
    Sort as LogicalSort, Source,
};
use crate::logical_plan::LogicalPlan;
use crate::partitioning::{
    ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
};
use crate::physical_plan::PhysicalPlan;
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::SourceInfo;
use crate::FileFormat;
use crate::{physical_ops::*, JoinStrategy};

#[cfg(feature = "python")]
use crate::physical_ops::InMemoryScan;

/// Translate a logical plan to a physical plan.
pub fn plan(logical_plan: &LogicalPlan, cfg: Arc<DaftExecutionConfig>) -> DaftResult<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Source(Source { source_info, .. }) => match source_info.as_ref() {
            SourceInfo::ExternalInfo(ScanExternalInfo {
                pushdowns,
                scan_op,
                source_schema,
                ..
            }) => {
                let scan_tasks = scan_op.0.to_scan_tasks(pushdowns.clone())?;

                let scan_tasks = daft_scan::scan_task_iters::split_by_row_groups(
                    scan_tasks,
                    cfg.parquet_split_row_groups_max_files,
                    cfg.scan_tasks_min_size_bytes,
                    cfg.scan_tasks_max_size_bytes,
                );

                // Apply transformations on the ScanTasks to optimize
                let scan_tasks = daft_scan::scan_task_iters::merge_by_sizes(
                    scan_tasks,
                    cfg.scan_tasks_min_size_bytes,
                    cfg.scan_tasks_max_size_bytes,
                );
                let scan_tasks = scan_tasks.collect::<DaftResult<Vec<_>>>()?;
                if scan_tasks.is_empty() {
                    let clustering_spec =
                        Arc::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)));

                    Ok(PhysicalPlan::EmptyScan(EmptyScan::new(
                        source_schema.clone(),
                        clustering_spec,
                    )))
                } else {
                    let clustering_spec = Arc::new(ClusteringSpec::Unknown(
                        UnknownClusteringConfig::new(scan_tasks.len()),
                    ));

                    Ok(PhysicalPlan::TabularScan(TabularScan::new(
                        scan_tasks,
                        clustering_spec,
                    )))
                }
            }
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(mem_info) => {
                let scan = PhysicalPlan::InMemoryScan(InMemoryScan::new(
                    mem_info.source_schema.clone(),
                    mem_info.clone(),
                    ClusteringSpec::Unknown(UnknownClusteringConfig::new(mem_info.num_partitions))
                        .into(),
                ));
                Ok(scan)
            }
        },
        LogicalPlan::Project(LogicalProject {
            input,
            projection,
            resource_request,
            ..
        }) => {
            let input_physical = plan(input, cfg)?;
            let clustering_spec = input_physical.clustering_spec().clone();
            Ok(PhysicalPlan::Project(Project::try_new(
                input_physical.into(),
                projection.clone(),
                resource_request.clone(),
                clustering_spec,
            )?))
        }
        LogicalPlan::Filter(LogicalFilter { input, predicate }) => {
            let input_physical = plan(input, cfg)?;
            Ok(PhysicalPlan::Filter(Filter::new(
                input_physical.into(),
                predicate.clone(),
            )))
        }
        LogicalPlan::Limit(LogicalLimit {
            input,
            limit,
            eager,
        }) => {
            let input_physical = plan(input, cfg)?;
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(PhysicalPlan::Limit(Limit::new(
                input_physical.into(),
                *limit,
                *eager,
                num_partitions,
            )))
        }
        LogicalPlan::Explode(LogicalExplode {
            input, to_explode, ..
        }) => {
            let input_physical = plan(input, cfg)?;
            Ok(PhysicalPlan::Explode(Explode::try_new(
                input_physical.into(),
                to_explode.clone(),
            )?))
        }
        LogicalPlan::Sort(LogicalSort {
            input,
            sort_by,
            descending,
        }) => {
            let input_physical = plan(input, cfg)?;
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(PhysicalPlan::Sort(Sort::new(
                input_physical.into(),
                sort_by.clone(),
                descending.clone(),
                num_partitions,
            )))
        }
        LogicalPlan::Repartition(LogicalRepartition {
            input,
            repartition_spec,
        }) => {
            let input_physical = plan(input, cfg)?;
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
                return Ok(input_physical);
            }
            let input_physical = Arc::new(input_physical);
            let repartitioned_plan = match clustering_spec {
                ClusteringSpec::Unknown(_) => {
                    match num_partitions.cmp(&input_num_partitions) {
                        Ordering::Greater => {
                            // Split input partitions into num_partitions.
                            let split_op = PhysicalPlan::Split(Split::new(
                                input_physical,
                                input_num_partitions,
                                num_partitions,
                            ));
                            PhysicalPlan::Flatten(Flatten::new(split_op.into()))
                        }
                        Ordering::Less => {
                            // Coalesce input partitions into num_partitions.
                            PhysicalPlan::Coalesce(Coalesce::new(
                                input_physical,
                                input_num_partitions,
                                num_partitions,
                            ))
                        }
                        Ordering::Equal => {
                            // # of output partitions == # of input partitions; this should have already short-circuited with
                            // a repartition drop above.
                            unreachable!("Simple repartitioning with same # of output partitions as the input; this should have been dropped.")
                        }
                    }
                }
                ClusteringSpec::Random(_) => {
                    let split_op = PhysicalPlan::FanoutRandom(FanoutRandom::new(
                        input_physical,
                        num_partitions,
                    ));
                    PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                }
                ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => {
                    let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                        input_physical,
                        num_partitions,
                        by.clone(),
                    ));
                    PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                }
                ClusteringSpec::Range(_) => {
                    unreachable!("Repartitioning by range is not supported")
                }
            };
            Ok(repartitioned_plan)
        }
        LogicalPlan::Distinct(LogicalDistinct { input }) => {
            let input_physical = plan(input, cfg)?;
            let col_exprs = input
                .schema()
                .names()
                .iter()
                .map(|name| Expr::Column(name.clone().into()))
                .collect::<Vec<Expr>>();
            let agg_op = PhysicalPlan::Aggregate(Aggregate::new(
                input_physical.into(),
                vec![],
                col_exprs.clone(),
            ));
            let num_partitions = agg_op.clustering_spec().num_partitions();
            if num_partitions > 1 {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    agg_op.into(),
                    num_partitions,
                    col_exprs.clone(),
                ));
                let reduce_op = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
                Ok(PhysicalPlan::Aggregate(Aggregate::new(
                    reduce_op.into(),
                    vec![],
                    col_exprs,
                )))
            } else {
                Ok(agg_op)
            }
        }
        LogicalPlan::Sample(LogicalSample {
            input,
            fraction,
            with_replacement,
            seed,
        }) => {
            let input_physical = plan(input, cfg)?;
            Ok(PhysicalPlan::Sample(Sample::new(
                input_physical.into(),
                *fraction,
                *with_replacement,
                *seed,
            )))
        }
        LogicalPlan::Aggregate(LogicalAggregate {
            aggregations,
            groupby,
            input,
            ..
        }) => {
            use daft_dsl::AggExpr::{self, *};
            use daft_dsl::Expr::Column;
            let input_plan = plan(input, cfg.clone())?;

            let num_input_partitions = input_plan.clustering_spec().num_partitions();

            let result_plan = match num_input_partitions {
                1 => PhysicalPlan::Aggregate(Aggregate::new(
                    input_plan.into(),
                    aggregations.clone(),
                    groupby.clone(),
                )),
                _ => {
                    let schema = logical_plan.schema();

                    // Aggregations to apply in the first and second stages.
                    // Semantic column name -> AggExpr
                    let mut first_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
                    let mut second_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
                    // Project the aggregation results to their final output names
                    let mut final_exprs: Vec<Expr> = groupby.clone();

                    for agg_expr in aggregations {
                        let output_name = agg_expr.name().unwrap();
                        match agg_expr {
                            Count(e, mode) => {
                                let count_id = agg_expr.semantic_id(&schema).id;
                                let sum_of_count_id =
                                    Sum(Column(count_id.clone()).into()).semantic_id(&schema).id;
                                first_stage_aggs.entry(count_id.clone()).or_insert(Count(
                                    e.alias(count_id.clone()).clone().into(),
                                    *mode,
                                ));
                                second_stage_aggs
                                    .entry(sum_of_count_id.clone())
                                    .or_insert(Sum(Column(count_id.clone())
                                        .alias(sum_of_count_id.clone())
                                        .into()));
                                final_exprs
                                    .push(Column(sum_of_count_id.clone()).alias(output_name));
                            }
                            Sum(e) => {
                                let sum_id = agg_expr.semantic_id(&schema).id;
                                let sum_of_sum_id =
                                    Sum(Column(sum_id.clone()).into()).semantic_id(&schema).id;
                                first_stage_aggs
                                    .entry(sum_id.clone())
                                    .or_insert(Sum(e.alias(sum_id.clone()).clone().into()));
                                second_stage_aggs
                                    .entry(sum_of_sum_id.clone())
                                    .or_insert(Sum(Column(sum_id.clone())
                                        .alias(sum_of_sum_id.clone())
                                        .into()));
                                final_exprs.push(Column(sum_of_sum_id.clone()).alias(output_name));
                            }
                            Mean(e) => {
                                let sum_id = Sum(e.clone()).semantic_id(&schema).id;
                                let count_id =
                                    Count(e.clone(), CountMode::Valid).semantic_id(&schema).id;
                                let sum_of_sum_id =
                                    Sum(Column(sum_id.clone()).into()).semantic_id(&schema).id;
                                let sum_of_count_id =
                                    Sum(Column(count_id.clone()).into()).semantic_id(&schema).id;
                                first_stage_aggs
                                    .entry(sum_id.clone())
                                    .or_insert(Sum(e.alias(sum_id.clone()).clone().into()));
                                first_stage_aggs.entry(count_id.clone()).or_insert(Count(
                                    e.alias(count_id.clone()).clone().into(),
                                    CountMode::Valid,
                                ));
                                second_stage_aggs
                                    .entry(sum_of_sum_id.clone())
                                    .or_insert(Sum(Column(sum_id.clone())
                                        .alias(sum_of_sum_id.clone())
                                        .into()));
                                second_stage_aggs
                                    .entry(sum_of_count_id.clone())
                                    .or_insert(Sum(Column(count_id.clone())
                                        .alias(sum_of_count_id.clone())
                                        .into()));
                                final_exprs.push(
                                    (Column(sum_of_sum_id.clone())
                                        / Column(sum_of_count_id.clone()))
                                    .alias(output_name),
                                );
                            }
                            Min(e) => {
                                let min_id = agg_expr.semantic_id(&schema).id;
                                let min_of_min_id =
                                    Min(Column(min_id.clone()).into()).semantic_id(&schema).id;
                                first_stage_aggs
                                    .entry(min_id.clone())
                                    .or_insert(Min(e.alias(min_id.clone()).clone().into()));
                                second_stage_aggs
                                    .entry(min_of_min_id.clone())
                                    .or_insert(Min(Column(min_id.clone())
                                        .alias(min_of_min_id.clone())
                                        .into()));
                                final_exprs.push(Column(min_of_min_id.clone()).alias(output_name));
                            }
                            Max(e) => {
                                let max_id = agg_expr.semantic_id(&schema).id;
                                let max_of_max_id =
                                    Max(Column(max_id.clone()).into()).semantic_id(&schema).id;
                                first_stage_aggs
                                    .entry(max_id.clone())
                                    .or_insert(Max(e.alias(max_id.clone()).clone().into()));
                                second_stage_aggs
                                    .entry(max_of_max_id.clone())
                                    .or_insert(Max(Column(max_id.clone())
                                        .alias(max_of_max_id.clone())
                                        .into()));
                                final_exprs.push(Column(max_of_max_id.clone()).alias(output_name));
                            }
                            AnyValue(e, ignore_nulls) => {
                                let any_id = agg_expr.semantic_id(&schema).id;
                                let any_of_any_id =
                                    AnyValue(Column(any_id.clone()).into(), *ignore_nulls)
                                        .semantic_id(&schema)
                                        .id;
                                first_stage_aggs.entry(any_id.clone()).or_insert(AnyValue(
                                    e.alias(any_id.clone()).clone().into(),
                                    *ignore_nulls,
                                ));
                                second_stage_aggs
                                    .entry(any_of_any_id.clone())
                                    .or_insert(AnyValue(
                                        Column(any_id.clone()).alias(any_of_any_id.clone()).into(),
                                        *ignore_nulls,
                                    ));
                            }
                            List(e) => {
                                let list_id = agg_expr.semantic_id(&schema).id;
                                let concat_of_list_id = Concat(Column(list_id.clone()).into())
                                    .semantic_id(&schema)
                                    .id;
                                first_stage_aggs
                                    .entry(list_id.clone())
                                    .or_insert(List(e.alias(list_id.clone()).clone().into()));
                                second_stage_aggs
                                    .entry(concat_of_list_id.clone())
                                    .or_insert(Concat(
                                        Column(list_id.clone())
                                            .alias(concat_of_list_id.clone())
                                            .into(),
                                    ));
                                final_exprs
                                    .push(Column(concat_of_list_id.clone()).alias(output_name));
                            }
                            Concat(e) => {
                                let concat_id = agg_expr.semantic_id(&schema).id;
                                let concat_of_concat_id = Concat(Column(concat_id.clone()).into())
                                    .semantic_id(&schema)
                                    .id;
                                first_stage_aggs
                                    .entry(concat_id.clone())
                                    .or_insert(Concat(e.alias(concat_id.clone()).clone().into()));
                                second_stage_aggs
                                    .entry(concat_of_concat_id.clone())
                                    .or_insert(Concat(
                                        Column(concat_id.clone())
                                            .alias(concat_of_concat_id.clone())
                                            .into(),
                                    ));
                                final_exprs
                                    .push(Column(concat_of_concat_id.clone()).alias(output_name));
                            }
                            MapGroups { func, inputs } => {
                                let func_id = agg_expr.semantic_id(&schema).id;
                                // No first stage aggregation for MapGroups, do all the work in the second stage.
                                second_stage_aggs
                                    .entry(func_id.clone())
                                    .or_insert(MapGroups {
                                        func: func.clone(),
                                        inputs: inputs.to_vec(),
                                    });
                                final_exprs.push(Column(output_name.into()));
                            }
                        }
                    }

                    let first_stage_agg = if first_stage_aggs.is_empty() {
                        input_plan
                    } else {
                        PhysicalPlan::Aggregate(Aggregate::new(
                            input_plan.into(),
                            first_stage_aggs.values().cloned().collect(),
                            groupby.clone(),
                        ))
                    };
                    let gather_plan = if groupby.is_empty() {
                        PhysicalPlan::Coalesce(Coalesce::new(
                            first_stage_agg.into(),
                            num_input_partitions,
                            1,
                        ))
                    } else {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            first_stage_agg.into(),
                            min(
                                num_input_partitions,
                                cfg.shuffle_aggregation_default_partitions,
                            ),
                            groupby.clone(),
                        ));
                        PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan.into(),
                        second_stage_aggs.values().cloned().collect(),
                        groupby.clone(),
                    ));

                    let clustering_spec = second_stage_agg.clustering_spec().clone();
                    PhysicalPlan::Project(Project::try_new(
                        second_stage_agg.into(),
                        final_exprs,
                        Default::default(),
                        clustering_spec,
                    )?)
                }
            };

            Ok(result_plan)
        }
        LogicalPlan::Concat(LogicalConcat { input, other }) => {
            let input_physical = plan(input, cfg.clone())?;
            let other_physical = plan(other, cfg.clone())?;
            Ok(PhysicalPlan::Concat(Concat::new(
                input_physical.into(),
                other_physical.into(),
            )))
        }
        LogicalPlan::Join(LogicalJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            join_strategy,
            output_schema,
            ..
        }) => {
            let mut left_physical = plan(left, cfg.clone())?;
            let mut right_physical = plan(right, cfg.clone())?;

            let left_clustering_spec = left_physical.clustering_spec();
            let right_clustering_spec = right_physical.clustering_spec();
            let num_partitions = max(
                left_clustering_spec.num_partitions(),
                right_clustering_spec.num_partitions(),
            );
            let new_left_hash_clustering_spec = Arc::new(ClusteringSpec::Hash(
                HashClusteringConfig::new(num_partitions, left_on.clone()),
            ));
            let new_right_hash_clustering_spec = Arc::new(ClusteringSpec::Hash(
                HashClusteringConfig::new(num_partitions, right_on.clone()),
            ));

            let is_left_hash_partitioned = left_clustering_spec == new_left_hash_clustering_spec;
            let is_right_hash_partitioned = right_clustering_spec == new_right_hash_clustering_spec;

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

            // For broadcast joins, ensure that the left side of the join is the smaller side.
            let (smaller_size_bytes, left_is_larger) = match (
                left_physical.approximate_size_bytes(),
                right_physical.approximate_size_bytes(),
            ) {
                (Some(left_size_bytes), Some(right_size_bytes)) => {
                    if right_size_bytes < left_size_bytes {
                        (Some(right_size_bytes), true)
                    } else {
                        (Some(left_size_bytes), false)
                    }
                }
                (Some(left_size_bytes), None) => (Some(left_size_bytes), false),
                (None, Some(right_size_bytes)) => (Some(right_size_bytes), true),
                (None, None) => (None, false),
            };
            let is_larger_partitioned = if left_is_larger {
                is_left_hash_partitioned || is_left_sort_partitioned
            } else {
                is_right_hash_partitioned || is_right_sort_partitioned
            };
            let join_strategy = join_strategy.unwrap_or_else(|| {
                let is_primitive = |exprs: &Vec<Expr>| exprs.iter().map(|e| e.name().unwrap()).all(|col| {
                    let dtype = &output_schema.get_field(col).unwrap().dtype;
                    dtype.is_integer() || dtype.is_floating() || matches!(dtype, DataType::Utf8 | DataType::Binary | DataType::Boolean)
                });
                // If larger table is not already partitioned on the join key AND the smaller table is under broadcast size threshold, use broadcast join.
                if !is_larger_partitioned && let Some(smaller_size_bytes) = smaller_size_bytes && smaller_size_bytes <= cfg.broadcast_join_size_bytes_threshold {
                    JoinStrategy::Broadcast
                // Larger side of join is range-partitioned on the join column, so we use a sort-merge join.
                // TODO(Clark): Support non-primitive dtypes for sort-merge join (e.g. temporal types).
                // TODO(Clark): Also do a sort-merge join if a downstream op needs the table to be sorted on the join key.
                // TODO(Clark): Look into defaulting to sort-merge join over hash join under more input partitioning setups.
                } else if is_primitive(left_on) && is_primitive(right_on) && (is_left_sort_partitioned || is_right_sort_partitioned)
                && (!is_larger_partitioned
                    || (left_is_larger && is_left_sort_partitioned
                        || !left_is_larger && is_right_sort_partitioned)) {
                            JoinStrategy::SortMerge
                // Otherwise, use a hash join.
                } else {
                    JoinStrategy::Hash
                }
            });
            match join_strategy {
                JoinStrategy::Broadcast => {
                    // If either the left or right side of the join are very small tables, perform a broadcast join with the
                    // entire smaller table broadcast to each of the partitions of the larger table.
                    if left_is_larger {
                        // These will get swapped back when doing the actual local joins.
                        (left_physical, right_physical) = (right_physical, left_physical);
                    }
                    Ok(PhysicalPlan::BroadcastJoin(BroadcastJoin::new(
                        left_physical.into(),
                        right_physical.into(),
                        left_on.clone(),
                        right_on.clone(),
                        *join_type,
                        left_is_larger,
                    )))
                }
                JoinStrategy::SortMerge => {
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
                                left_physical.into(),
                                left_on.clone(),
                                std::iter::repeat(false).take(left_on.len()).collect(),
                                num_partitions,
                            ))
                        }
                        if !is_right_sort_partitioned {
                            right_physical = PhysicalPlan::Sort(Sort::new(
                                right_physical.into(),
                                right_on.clone(),
                                std::iter::repeat(false).take(right_on.len()).collect(),
                                num_partitions,
                            ))
                        }
                        false
                    };
                    Ok(PhysicalPlan::SortMergeJoin(SortMergeJoin::new(
                        left_physical.into(),
                        right_physical.into(),
                        left_on.clone(),
                        right_on.clone(),
                        *join_type,
                        num_partitions,
                        left_is_larger,
                        needs_presort,
                    )))
                }
                JoinStrategy::Hash => {
                    if (num_partitions > 1
                        || left_clustering_spec.num_partitions() != num_partitions)
                        && !is_left_hash_partitioned
                    {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            left_physical.into(),
                            num_partitions,
                            left_on.clone(),
                        ));
                        left_physical =
                            PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
                    }
                    if (num_partitions > 1
                        || right_clustering_spec.num_partitions() != num_partitions)
                        && !is_right_hash_partitioned
                    {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            right_physical.into(),
                            num_partitions,
                            right_on.clone(),
                        ));
                        right_physical =
                            PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
                    }
                    Ok(PhysicalPlan::HashJoin(HashJoin::new(
                        left_physical.into(),
                        right_physical.into(),
                        left_on.clone(),
                        right_on.clone(),
                        *join_type,
                    )))
                }
            }
        }
        LogicalPlan::Sink(LogicalSink {
            schema,
            sink_info,
            input,
        }) => {
            let input_physical = plan(input, cfg)?;
            match sink_info.as_ref() {
                SinkInfo::OutputFileInfo(file_info @ OutputFileInfo { file_format, .. }) => {
                    match file_format {
                        FileFormat::Parquet => {
                            Ok(PhysicalPlan::TabularWriteParquet(TabularWriteParquet::new(
                                schema.clone(),
                                file_info.clone(),
                                input_physical.into(),
                            )))
                        }
                        FileFormat::Csv => Ok(PhysicalPlan::TabularWriteCsv(TabularWriteCsv::new(
                            schema.clone(),
                            file_info.clone(),
                            input_physical.into(),
                        ))),
                        FileFormat::Json => {
                            Ok(PhysicalPlan::TabularWriteJson(TabularWriteJson::new(
                                schema.clone(),
                                file_info.clone(),
                                input_physical.into(),
                            )))
                        }
                    }
                }
            }
        }
        LogicalPlan::MonotonicallyIncreasingId(LogicalMonotonicallyIncreasingId {
            input,
            column_name,
            ..
        }) => {
            let input_physical = plan(input, cfg)?;
            Ok(PhysicalPlan::MonotonicallyIncreasingId(
                MonotonicallyIncreasingId::new(input_physical.into(), column_name),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit, AggExpr, Expr};
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use crate::physical_plan::PhysicalPlan;
    use crate::planner::plan;
    use crate::test::{dummy_scan_node, dummy_scan_operator};

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
        .filter(col("a").lt(&lit(2)))?;
        assert_eq!(
            plan(builder.build().as_ref(), cfg.clone())?
                .clustering_spec()
                .num_partitions(),
            10
        );
        let logical_plan = builder.into_partitions(10)?.build();
        let physical_plan = plan(logical_plan.as_ref(), cfg.clone())?;
        // Check that the last repartition was dropped (the last op should be the filter).
        assert_matches!(physical_plan, PhysicalPlan::Filter(_));
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
            plan(builder.build().as_ref(), cfg.clone())?
                .clustering_spec()
                .num_partitions(),
            1
        );
        let logical_plan = builder.hash_repartition(Some(1), vec![col("a")])?.build();
        let physical_plan = plan(logical_plan.as_ref(), cfg.clone())?;
        assert_matches!(physical_plan, PhysicalPlan::TabularScan(_));
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
        .hash_repartition(Some(10), vec![col("a")])?
        .filter(col("a").lt(&lit(2)))?
        .hash_repartition(Some(10), vec![col("a")])?
        .build();
        let physical_plan = plan(logical_plan.as_ref(), cfg)?;
        // Check that the last repartition was dropped (the last op should be the filter).
        assert_matches!(physical_plan, PhysicalPlan::Filter(_));
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
        .hash_repartition(Some(10), vec![col("a")])?
        .aggregate(
            vec![Expr::Agg(AggExpr::Sum(col("a").into()))],
            vec![col("b")],
        )?
        .hash_repartition(Some(10), vec![col("b")])?
        .build();
        let physical_plan = plan(logical_plan.as_ref(), cfg)?;
        // Check that the last repartition was dropped (the last op should be a projection for a multi-partition aggregation).
        assert_matches!(physical_plan, PhysicalPlan::Project(_));
        Ok(())
    }
}
