use std::{
    cmp::{max, min, Ordering},
    collections::HashMap,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_scan_info::PhysicalScanInfo;
use daft_core::prelude::*;
use daft_dsl::{
    col, functions::agg::merge_mean, is_partition_compatible, AggExpr, ApproxPercentileParams,
    Expr, ExprRef, SketchType,
};
use daft_functions::numeric::sqrt;
use daft_logical_plan::{
    logical_plan::LogicalPlan,
    ops::{
        ActorPoolProject as LogicalActorPoolProject, Aggregate as LogicalAggregate,
        Distinct as LogicalDistinct, Explode as LogicalExplode, Filter as LogicalFilter,
        Join as LogicalJoin, Limit as LogicalLimit,
        MonotonicallyIncreasingId as LogicalMonotonicallyIncreasingId, Pivot as LogicalPivot,
        Project as LogicalProject, Repartition as LogicalRepartition, Sample as LogicalSample,
        Sink as LogicalSink, Sort as LogicalSort, Source, Unpivot as LogicalUnpivot,
    },
    partitioning::{
        ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
    },
    sink_info::{OutputFileInfo, SinkInfo},
    source_info::{PlaceHolderInfo, SourceInfo},
};

use crate::{ops::*, PhysicalPlan, PhysicalPlanRef};

pub(super) fn translate_single_logical_node(
    logical_plan: &LogicalPlan,
    physical_children: &mut Vec<PhysicalPlanRef>,
    cfg: &DaftExecutionConfig,
) -> DaftResult<PhysicalPlanRef> {
    match logical_plan {
        LogicalPlan::Source(Source { source_info, .. }) => match source_info.as_ref() {
            SourceInfo::Physical(PhysicalScanInfo {
                pushdowns,
                scan_op,
                source_schema,
                ..
            }) => {
                let scan_tasks = scan_op.0.to_scan_tasks(pushdowns.clone(), Some(cfg))?;

                if scan_tasks.is_empty() {
                    let clustering_spec =
                        Arc::new(ClusteringSpec::Unknown(UnknownClusteringConfig::new(1)));

                    Ok(PhysicalPlan::EmptyScan(EmptyScan::new(
                        source_schema.clone(),
                        clustering_spec,
                    ))
                    .arced())
                } else {
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
            SourceInfo::PlaceHolder(PlaceHolderInfo { source_id, .. }) => {
                panic!("Placeholder {source_id} should not get to translation. This should have been optimized away");
            }
        },
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
        LogicalPlan::Filter(LogicalFilter { predicate, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            Ok(PhysicalPlan::Filter(Filter::new(input_physical, predicate.clone())).arced())
        }
        LogicalPlan::Limit(LogicalLimit { limit, eager, .. }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(
                PhysicalPlan::Limit(Limit::new(input_physical, *limit, *eager, num_partitions))
                    .arced(),
            )
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
                return Ok(input_physical);
            }
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
                        .get_random_partitioning(num_partitions, Some(cfg)),
                ),
                ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => {
                    PhysicalPlan::ShuffleExchange(
                        ShuffleExchangeFactory::new(input_physical).get_hash_partitioning(
                            by,
                            num_partitions,
                            Some(cfg),
                        ),
                    )
                }
                ClusteringSpec::Range(_) => {
                    unreachable!("Repartitioning by range is not supported")
                }
            };
            Ok(repartitioned_plan.arced())
        }
        LogicalPlan::Distinct(LogicalDistinct { input }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let col_exprs = input
                .schema()
                .names()
                .iter()
                .map(|name| daft_dsl::col(name.clone()))
                .collect::<Vec<ExprRef>>();
            let agg_op =
                PhysicalPlan::Aggregate(Aggregate::new(input_physical, vec![], col_exprs.clone()));
            let num_partitions = agg_op.clustering_spec().num_partitions();
            if num_partitions > 1 {
                let shuffle_op = PhysicalPlan::ShuffleExchange(
                    ShuffleExchangeFactory::new(agg_op.into()).get_hash_partitioning(
                        col_exprs.clone(),
                        num_partitions,
                        Some(cfg),
                    ),
                );
                Ok(
                    PhysicalPlan::Aggregate(Aggregate::new(shuffle_op.into(), vec![], col_exprs))
                        .arced(),
                )
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
                            groupby.iter().map(|e| col(e.name())).collect(),
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
                            ),
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
                            ),
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
        LogicalPlan::Join(LogicalJoin {
            left,
            right,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            join_strategy,
            ..
        }) => {
            if left_on.is_empty() && right_on.is_empty() && join_type == &JoinType::Inner {
                return Err(DaftError::not_implemented(
                    "Joins without join conditions (cross join) are not supported yet",
                ));
            }
            let mut right_physical = physical_children.pop().expect("requires 1 inputs");
            let mut left_physical = physical_children.pop().expect("requires 2 inputs");

            let left_clustering_spec = left_physical.clustering_spec();
            let right_clustering_spec = right_physical.clustering_spec();
            let num_partitions = max(
                left_clustering_spec.num_partitions(),
                right_clustering_spec.num_partitions(),
            );

            let is_left_hash_partitioned =
                matches!(left_clustering_spec.as_ref(), ClusteringSpec::Hash(..))
                    && is_partition_compatible(&left_clustering_spec.partition_by(), left_on);
            let is_right_hash_partitioned =
                matches!(right_clustering_spec.as_ref(), ClusteringSpec::Hash(..))
                    && is_partition_compatible(&right_clustering_spec.partition_by(), right_on);

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
            let (smaller_size_bytes, left_is_larger) =
                match (left_stats.upper_bound_bytes, right_stats.upper_bound_bytes) {
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
            let has_null_safe_equals = null_equals_nulls
                .as_ref()
                .map_or(false, |v| v.iter().any(|b| *b));
            let join_strategy = join_strategy.unwrap_or_else(|| {
                fn keys_are_primitive(on: &[ExprRef], schema: &SchemaRef) -> bool {
                    on.iter().all(|expr| {
                        let dtype = expr.get_type(schema).unwrap();
                        dtype.is_integer()
                            || dtype.is_floating()
                            || matches!(
                                dtype,
                                DataType::Utf8 | DataType::Binary | DataType::Boolean
                            )
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
                    && let Some(smaller_size_bytes) = smaller_size_bytes
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
                    && keys_are_primitive(left_on, &left.schema())
                    && keys_are_primitive(right_on, &right.schema())
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

                    Ok(PhysicalPlan::BroadcastJoin(BroadcastJoin::new(
                        left_physical,
                        right_physical,
                        left_on.clone(),
                        right_on.clone(),
                        null_equals_nulls.clone(),
                        *join_type,
                        is_swapped,
                    ))
                    .arced())
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
                                std::iter::repeat(false).take(left_on.len()).collect(),
                                std::iter::repeat(false).take(left_on.len()).collect(),
                                num_partitions,
                            ))
                            .arced();
                        }
                        if !is_right_sort_partitioned {
                            right_physical = PhysicalPlan::Sort(Sort::new(
                                right_physical,
                                right_on.clone(),
                                std::iter::repeat(false).take(right_on.len()).collect(),
                                std::iter::repeat(false).take(right_on.len()).collect(),
                                num_partitions,
                            ))
                            .arced();
                        }
                        false
                    };
                    Ok(PhysicalPlan::SortMergeJoin(SortMergeJoin::new(
                        left_physical,
                        right_physical,
                        left_on.clone(),
                        right_on.clone(),
                        *join_type,
                        num_partitions,
                        left_is_larger,
                        needs_presort,
                    ))
                    .arced())
                }
                JoinStrategy::Hash => {
                    // allow for leniency in partition size to avoid minor repartitions
                    let num_left_partitions = left_clustering_spec.num_partitions();
                    let num_right_partitions = right_clustering_spec.num_partitions();

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
                            ),
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
                            ),
                        )
                        .into();
                    }
                    Ok(PhysicalPlan::HashJoin(HashJoin::new(
                        left_physical,
                        right_physical,
                        left_on.clone(),
                        right_on.clone(),
                        null_equals_nulls.clone(),
                        *join_type,
                    ))
                    .arced())
                }
            }
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
                AggExpr::AnyValue(e, ignore_nulls) => {
                    AggExpr::AnyValue(Expr::Alias(e, name.clone()).into(), ignore_nulls)
                }
                AggExpr::List(e) => AggExpr::List(Expr::Alias(e, name.clone()).into()),
                AggExpr::Concat(e) => AggExpr::Concat(Expr::Alias(e, name.clone()).into()),
                AggExpr::MapGroups { func, inputs } => AggExpr::MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        _ => Err(DaftError::InternalError("Expected non-agg expressions in aggregation to be factored out before plan translation.".to_string())),
    }
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
    let mut final_exprs: Vec<ExprRef> = group_by.iter().map(|e| col(e.name())).collect();

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
                let sum_of_count_id = AggExpr::Sum(col(count_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(count_id.clone())
                    .or_insert(AggExpr::Count(e.alias(count_id.clone()).clone(), *mode));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(AggExpr::Sum(
                        col(count_id.clone()).alias(sum_of_count_id.clone()),
                    ));
                final_exprs.push(col(sum_of_count_id.clone()).alias(output_name));
            }
            AggExpr::Sum(e) => {
                let sum_id = agg_expr.semantic_id(schema).id;
                let sum_of_sum_id = AggExpr::Sum(col(sum_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(sum_id.clone())
                    .or_insert(AggExpr::Sum(e.alias(sum_id.clone()).clone()));
                second_stage_aggs
                    .entry(sum_of_sum_id.clone())
                    .or_insert(AggExpr::Sum(
                        col(sum_id.clone()).alias(sum_of_sum_id.clone()),
                    ));
                final_exprs.push(col(sum_of_sum_id.clone()).alias(output_name));
            }
            AggExpr::Mean(e) => {
                let sum_id = AggExpr::Sum(e.clone()).semantic_id(schema).id;
                let count_id = AggExpr::Count(e.clone(), CountMode::Valid)
                    .semantic_id(schema)
                    .id;
                let sum_of_sum_id = AggExpr::Sum(col(sum_id.clone())).semantic_id(schema).id;
                let sum_of_count_id = AggExpr::Sum(col(count_id.clone())).semantic_id(schema).id;
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
                        col(sum_id.clone()).alias(sum_of_sum_id.clone()),
                    ));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(AggExpr::Sum(
                        col(count_id.clone()).alias(sum_of_count_id.clone()),
                    ));
                final_exprs.push(
                    merge_mean(col(sum_of_sum_id.clone()), col(sum_of_count_id.clone()))
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
                    col(sum_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );
                let global_sq_sum_id = add_to_stage(
                    AggExpr::Sum,
                    col(sq_sum_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );
                let global_count_id = add_to_stage(
                    AggExpr::Sum,
                    col(count_id.clone()),
                    schema,
                    &mut second_stage_aggs,
                );

                // final projection
                let g_sq_sum = col(global_sq_sum_id);
                let g_sum = col(global_sum_id);
                let g_count = col(global_count_id);
                let left = g_sq_sum.div(g_count.clone());
                let right = g_sum.div(g_count);
                let right = right.clone().mul(right);
                let result = sqrt::sqrt(left.sub(right)).alias(output_name);

                final_exprs.push(result);
            }
            AggExpr::Min(e) => {
                let min_id = agg_expr.semantic_id(schema).id;
                let min_of_min_id = AggExpr::Min(col(min_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(min_id.clone())
                    .or_insert(AggExpr::Min(e.alias(min_id.clone()).clone()));
                second_stage_aggs
                    .entry(min_of_min_id.clone())
                    .or_insert(AggExpr::Min(
                        col(min_id.clone()).alias(min_of_min_id.clone()),
                    ));
                final_exprs.push(col(min_of_min_id.clone()).alias(output_name));
            }
            AggExpr::Max(e) => {
                let max_id = agg_expr.semantic_id(schema).id;
                let max_of_max_id = AggExpr::Max(col(max_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(max_id.clone())
                    .or_insert(AggExpr::Max(e.alias(max_id.clone()).clone()));
                second_stage_aggs
                    .entry(max_of_max_id.clone())
                    .or_insert(AggExpr::Max(
                        col(max_id.clone()).alias(max_of_max_id.clone()),
                    ));
                final_exprs.push(col(max_of_max_id.clone()).alias(output_name));
            }
            AggExpr::AnyValue(e, ignore_nulls) => {
                let any_id = agg_expr.semantic_id(schema).id;
                let any_of_any_id = AggExpr::AnyValue(col(any_id.clone()), *ignore_nulls)
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
                        col(any_id.clone()).alias(any_of_any_id.clone()),
                        *ignore_nulls,
                    ));
                final_exprs.push(col(any_of_any_id.clone()).alias(output_name));
            }
            AggExpr::List(e) => {
                let list_id = agg_expr.semantic_id(schema).id;
                let concat_of_list_id =
                    AggExpr::Concat(col(list_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(list_id.clone())
                    .or_insert(AggExpr::List(e.alias(list_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_list_id.clone())
                    .or_insert(AggExpr::Concat(
                        col(list_id.clone()).alias(concat_of_list_id.clone()),
                    ));
                final_exprs.push(col(concat_of_list_id.clone()).alias(output_name));
            }
            AggExpr::Concat(e) => {
                let concat_id = agg_expr.semantic_id(schema).id;
                let concat_of_concat_id = AggExpr::Concat(col(concat_id.clone()))
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(concat_id.clone())
                    .or_insert(AggExpr::Concat(e.alias(concat_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_concat_id.clone())
                    .or_insert(AggExpr::Concat(
                        col(concat_id.clone()).alias(concat_of_concat_id.clone()),
                    ));
                final_exprs.push(col(concat_of_concat_id.clone()).alias(output_name));
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
                final_exprs.push(col(output_name));
            }
            &AggExpr::ApproxPercentile(ApproxPercentileParams {
                child: ref e,
                ref percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                let sketch_id = agg_expr.semantic_id(schema).id;
                let approx_id = AggExpr::ApproxSketch(col(sketch_id.clone()), SketchType::DDSketch)
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
                        col(sketch_id.clone()).alias(approx_id.clone()),
                        SketchType::DDSketch,
                    ));
                final_exprs.push(
                    col(approx_id)
                        .sketch_percentile(percentiles.as_slice(), force_list_output)
                        .alias(output_name),
                );
            }
            AggExpr::ApproxCountDistinct(e) => {
                let first_stage_id = agg_expr.semantic_id(schema).id;
                let second_stage_id =
                    AggExpr::MergeSketch(col(first_stage_id.clone()), SketchType::HyperLogLog)
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
                        col(first_stage_id).alias(second_stage_id.clone()),
                        SketchType::HyperLogLog,
                    ));
                final_exprs.push(col(second_stage_id).alias(output_name));
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

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, sync::Arc};

    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{col, lit};
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
        .filter(col("a").lt(lit(2)))?;
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
        let logical_plan = builder.hash_repartition(Some(1), vec![col("a")])?.build();
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
        .hash_repartition(Some(10), vec![col("a")])?
        .filter(col("a").lt(lit(2)))?
        .hash_repartition(Some(10), vec![col("a")])?
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
        .hash_repartition(Some(10), vec![col("a")])?
        .aggregate(vec![col("a").sum()], vec![col("b")])?
        .hash_repartition(Some(10), vec![col("b")])?
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
            RepartitionOptions::Good(x) => node.hash_repartition(Some(x), vec![col("a"), col("b")]),
            RepartitionOptions::Bad(x) => node.hash_repartition(Some(x), vec![col("a"), col("c")]),
            RepartitionOptions::Reversed(x) => {
                node.hash_repartition(Some(x), vec![col("b"), col("a")])
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
            col("a"),
            col("b"),
            col("c").alias("dataR"),
        ])?;

        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]));
        let logical_plan = force_repartition(logical_plan, left_partitions)?
            .select(vec![col("a"), col("b"), col("c").alias("dataL")])?
            .join(
                join_node,
                vec![col("a"), col("b")],
                vec![col("a"), col("b")],
                JoinType::Inner,
                Some(JoinStrategy::Hash),
                None,
                None,
                false,
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
