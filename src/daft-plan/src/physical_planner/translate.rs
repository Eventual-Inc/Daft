use std::cmp::Ordering;
use std::sync::Arc;
use std::{
    cmp::{max, min},
    collections::HashMap,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;

use daft_core::count_mode::CountMode;
use daft_core::join::{JoinStrategy, JoinType};
use daft_core::schema::SchemaRef;
use daft_core::DataType;
use daft_dsl::ExprRef;
use daft_dsl::{col, ApproxPercentileParams};

use daft_scan::PhysicalScanInfo;

use crate::logical_ops::{
    Aggregate as LogicalAggregate, Distinct as LogicalDistinct, Explode as LogicalExplode,
    Filter as LogicalFilter, Join as LogicalJoin, Limit as LogicalLimit,
    MonotonicallyIncreasingId as LogicalMonotonicallyIncreasingId, Pivot as LogicalPivot,
    Project as LogicalProject, Repartition as LogicalRepartition, Sample as LogicalSample,
    Sink as LogicalSink, Sort as LogicalSort, Source, Unpivot as LogicalUnpivot,
};
use crate::logical_plan::LogicalPlan;
use crate::partitioning::{
    ClusteringSpec, HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig,
};
use crate::physical_ops::*;
use crate::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::{PlaceHolderInfo, SourceInfo};
use crate::FileFormat;

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
                let scan_tasks = scan_op.0.to_scan_tasks(pushdowns.clone())?;

                let scan_tasks = daft_scan::scan_task_iters::split_by_row_groups(
                    scan_tasks,
                    cfg.parquet_split_row_groups_max_files,
                    cfg.scan_tasks_min_size_bytes,
                    cfg.scan_tasks_max_size_bytes,
                );

                // Apply transformations on the ScanTasks to optimize
                let scan_tasks = daft_scan::scan_task_iters::merge_by_sizes(scan_tasks, cfg);
                let scan_tasks = scan_tasks.collect::<DaftResult<Vec<_>>>()?;
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
        LogicalPlan::Project(LogicalProject {
            projection,
            resource_request,
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let clustering_spec = input_physical.clustering_spec().clone();
            Ok(PhysicalPlan::Project(Project::try_new(
                input_physical,
                projection.clone(),
                resource_request.clone(),
                clustering_spec,
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
            ..
        }) => {
            let input_physical = physical_children.pop().expect("requires 1 input");
            let num_partitions = input_physical.clustering_spec().num_partitions();
            Ok(PhysicalPlan::Sort(Sort::new(
                input_physical,
                sort_by.clone(),
                descending.clone(),
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
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    agg_op.into(),
                    num_partitions,
                    col_exprs.clone(),
                ));
                let reduce_op = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
                Ok(
                    PhysicalPlan::Aggregate(Aggregate::new(reduce_op.into(), vec![], col_exprs))
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

            let result_plan = match num_input_partitions {
                1 => PhysicalPlan::Aggregate(Aggregate::new(
                    input_physical,
                    aggregations.clone(),
                    groupby.clone(),
                )),
                _ => {
                    let schema = logical_plan.schema();

                    let (first_stage_aggs, second_stage_aggs, final_exprs) =
                        populate_aggregation_stages(aggregations, &schema, groupby);

                    let first_stage_agg = if first_stage_aggs.is_empty() {
                        input_physical
                    } else {
                        PhysicalPlan::Aggregate(Aggregate::new(
                            input_physical,
                            first_stage_aggs.values().cloned().collect(),
                            groupby.clone(),
                        ))
                        .arced()
                    };
                    let gather_plan = if groupby.is_empty() {
                        PhysicalPlan::Coalesce(Coalesce::new(
                            first_stage_agg,
                            num_input_partitions,
                            1,
                        ))
                        .arced()
                    } else {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            first_stage_agg,
                            min(
                                num_input_partitions,
                                cfg.shuffle_aggregation_default_partitions,
                            ),
                            groupby.clone(),
                        ))
                        .arced();
                        PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op)).arced()
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan,
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
                        PhysicalPlan::Coalesce(Coalesce::new(
                            first_stage_agg,
                            num_input_partitions,
                            1,
                        ))
                        .arced()
                    } else {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            first_stage_agg,
                            min(
                                num_input_partitions,
                                cfg.shuffle_aggregation_default_partitions,
                            ),
                            // NOTE: For the shuffle of a pivot operation, we don't include the pivot column for the hashing as we need
                            // to ensure that all rows with the same group_by column values are hashed to the same partition.
                            group_by.clone(),
                        ))
                        .arced();
                        PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op)).arced()
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan,
                        second_stage_aggs.values().cloned().collect(),
                        group_by_with_pivot,
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
            left_on,
            right_on,
            join_type,
            join_strategy,
            output_schema,
            ..
        }) => {
            let mut right_physical = physical_children.pop().expect("requires 1 inputs");
            let mut left_physical = physical_children.pop().expect("requires 2 inputs");

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
            let join_strategy = join_strategy.unwrap_or_else(|| {
                // This method will panic if called with columns that aren't in the output schema,
                // which is possible for anti- and semi-joins.
                let is_primitive = |exprs: &Vec<ExprRef>| {
                    exprs.iter().map(|e| e.name()).all(|col| {
                        let dtype = &output_schema.get_field(col).unwrap().dtype;
                        dtype.is_integer()
                            || dtype.is_floating()
                            || matches!(
                                dtype,
                                DataType::Utf8 | DataType::Binary | DataType::Boolean
                            )
                    })
                };
                // If larger table is not already partitioned on the join key AND the smaller table is under broadcast size threshold AND we are not broadcasting the side we are outer joining by, use broadcast join.
                if !is_larger_partitioned
                    && let Some(smaller_size_bytes) = smaller_size_bytes
                    && smaller_size_bytes <= cfg.broadcast_join_size_bytes_threshold
                    && (*join_type == JoinType::Inner
                        || (*join_type == JoinType::Left && left_is_larger)
                        || (*join_type == JoinType::Right && !left_is_larger))
                {
                    JoinStrategy::Broadcast
                // Larger side of join is range-partitioned on the join column, so we use a sort-merge join.
                // TODO(Clark): Support non-primitive dtypes for sort-merge join (e.g. temporal types).
                // TODO(Clark): Also do a sort-merge join if a downstream op needs the table to be sorted on the join key.
                // TODO(Clark): Look into defaulting to sort-merge join over hash join under more input partitioning setups.
                // TODO(Kevin): Support sort-merge join for other types of joins.
                } else if *join_type == JoinType::Inner
                    && is_primitive(left_on)
                    && is_primitive(right_on)
                    && (is_left_sort_partitioned || is_right_sort_partitioned)
                    && (!is_larger_partitioned
                        || (left_is_larger && is_left_sort_partitioned
                            || !left_is_larger && is_right_sort_partitioned))
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
                        (JoinType::Anti, _) => {
                            return Err(common_error::DaftError::ValueError(
                                "Broadcast join does not support anti joins.".to_string(),
                            ));
                        }
                        (JoinType::Semi, _) => {
                            return Err(common_error::DaftError::ValueError(
                                "Broadcast join does not support semi joins.".to_string(),
                            ));
                        }
                    };

                    if is_swapped {
                        (left_physical, right_physical) = (right_physical, left_physical);
                    }

                    Ok(PhysicalPlan::BroadcastJoin(BroadcastJoin::new(
                        left_physical,
                        right_physical,
                        left_on.clone(),
                        right_on.clone(),
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
                                num_partitions,
                            ))
                            .arced()
                        }
                        if !is_right_sort_partitioned {
                            right_physical = PhysicalPlan::Sort(Sort::new(
                                right_physical,
                                right_on.clone(),
                                std::iter::repeat(false).take(right_on.len()).collect(),
                                num_partitions,
                            ))
                            .arced()
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
                    if (num_partitions > 1
                        || left_clustering_spec.num_partitions() != num_partitions)
                        && !is_left_hash_partitioned
                    {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            left_physical,
                            num_partitions,
                            left_on.clone(),
                        ));
                        left_physical =
                            PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())).arced();
                    }
                    if (num_partitions > 1
                        || right_clustering_spec.num_partitions() != num_partitions)
                        && !is_right_hash_partitioned
                    {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            right_physical,
                            num_partitions,
                            right_on.clone(),
                        ));
                        right_physical =
                            PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())).arced();
                    }
                    Ok(PhysicalPlan::HashJoin(HashJoin::new(
                        left_physical,
                        right_physical,
                        left_on.clone(),
                        right_on.clone(),
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
                SinkInfo::CatalogInfo(catalog_info) => match &catalog_info.catalog {
                    crate::sink_info::CatalogType::Iceberg(iceberg_info) => {
                        Ok(PhysicalPlan::IcebergWrite(IcebergWrite::new(
                            schema.clone(),
                            iceberg_info.clone(),
                            input_physical,
                        ))
                        .arced())
                    }
                    crate::sink_info::CatalogType::DeltaLake(deltalake_info) => {
                        Ok(PhysicalPlan::DeltaLakeWrite(DeltaLakeWrite::new(
                            schema.clone(),
                            deltalake_info.clone(),
                            input_physical,
                        ))
                        .arced())
                    }
                    crate::sink_info::CatalogType::Lance(lance_info) => {
                        Ok(PhysicalPlan::LanceWrite(LanceWrite::new(
                            schema.clone(),
                            lance_info.clone(),
                            input_physical,
                        ))
                        .arced())
                    }
                },
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
    use daft_dsl::AggExpr::{self, *};

    // Aggregations to apply in the first and second stages.
    // Semantic column name -> AggExpr
    let mut first_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
    let mut second_stage_aggs: HashMap<Arc<str>, AggExpr> = HashMap::new();
    // Project the aggregation results to their final output names
    let mut final_exprs: Vec<ExprRef> = group_by.to_vec();

    for agg_expr in aggregations {
        let output_name = agg_expr.name();
        match agg_expr {
            Count(e, mode) => {
                let count_id = agg_expr.semantic_id(schema).id;
                let sum_of_count_id = Sum(col(count_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(count_id.clone())
                    .or_insert(Count(e.alias(count_id.clone()).clone(), *mode));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(Sum(col(count_id.clone()).alias(sum_of_count_id.clone())));
                final_exprs.push(col(sum_of_count_id.clone()).alias(output_name));
            }
            Sum(e) => {
                let sum_id = agg_expr.semantic_id(schema).id;
                let sum_of_sum_id = Sum(col(sum_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(sum_id.clone())
                    .or_insert(Sum(e.alias(sum_id.clone()).clone()));
                second_stage_aggs
                    .entry(sum_of_sum_id.clone())
                    .or_insert(Sum(col(sum_id.clone()).alias(sum_of_sum_id.clone())));
                final_exprs.push(col(sum_of_sum_id.clone()).alias(output_name));
            }
            Mean(e) => {
                let sum_id = Sum(e.clone()).semantic_id(schema).id;
                let count_id = Count(e.clone(), CountMode::Valid).semantic_id(schema).id;
                let sum_of_sum_id = Sum(col(sum_id.clone())).semantic_id(schema).id;
                let sum_of_count_id = Sum(col(count_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(sum_id.clone())
                    .or_insert(Sum(e.alias(sum_id.clone()).clone()));
                first_stage_aggs
                    .entry(count_id.clone())
                    .or_insert(Count(e.alias(count_id.clone()).clone(), CountMode::Valid));
                second_stage_aggs
                    .entry(sum_of_sum_id.clone())
                    .or_insert(Sum(col(sum_id.clone()).alias(sum_of_sum_id.clone())));
                second_stage_aggs
                    .entry(sum_of_count_id.clone())
                    .or_insert(Sum(col(count_id.clone()).alias(sum_of_count_id.clone())));
                final_exprs.push(
                    (col(sum_of_sum_id.clone()).div(col(sum_of_count_id.clone())))
                        .alias(output_name),
                );
            }
            Min(e) => {
                let min_id = agg_expr.semantic_id(schema).id;
                let min_of_min_id = Min(col(min_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(min_id.clone())
                    .or_insert(Min(e.alias(min_id.clone()).clone()));
                second_stage_aggs
                    .entry(min_of_min_id.clone())
                    .or_insert(Min(col(min_id.clone()).alias(min_of_min_id.clone())));
                final_exprs.push(col(min_of_min_id.clone()).alias(output_name));
            }
            Max(e) => {
                let max_id = agg_expr.semantic_id(schema).id;
                let max_of_max_id = Max(col(max_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(max_id.clone())
                    .or_insert(Max(e.alias(max_id.clone()).clone()));
                second_stage_aggs
                    .entry(max_of_max_id.clone())
                    .or_insert(Max(col(max_id.clone()).alias(max_of_max_id.clone())));
                final_exprs.push(col(max_of_max_id.clone()).alias(output_name));
            }
            AnyValue(e, ignore_nulls) => {
                let any_id = agg_expr.semantic_id(schema).id;
                let any_of_any_id = AnyValue(col(any_id.clone()), *ignore_nulls)
                    .semantic_id(schema)
                    .id;
                first_stage_aggs
                    .entry(any_id.clone())
                    .or_insert(AnyValue(e.alias(any_id.clone()).clone(), *ignore_nulls));
                second_stage_aggs
                    .entry(any_of_any_id.clone())
                    .or_insert(AnyValue(
                        col(any_id.clone()).alias(any_of_any_id.clone()),
                        *ignore_nulls,
                    ));
                final_exprs.push(col(any_of_any_id.clone()).alias(output_name));
            }
            List(e) => {
                let list_id = agg_expr.semantic_id(schema).id;
                let concat_of_list_id = Concat(col(list_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(list_id.clone())
                    .or_insert(List(e.alias(list_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_list_id.clone())
                    .or_insert(Concat(
                        col(list_id.clone()).alias(concat_of_list_id.clone()),
                    ));
                final_exprs.push(col(concat_of_list_id.clone()).alias(output_name));
            }
            Concat(e) => {
                let concat_id = agg_expr.semantic_id(schema).id;
                let concat_of_concat_id = Concat(col(concat_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(concat_id.clone())
                    .or_insert(Concat(e.alias(concat_id.clone()).clone()));
                second_stage_aggs
                    .entry(concat_of_concat_id.clone())
                    .or_insert(Concat(
                        col(concat_id.clone()).alias(concat_of_concat_id.clone()),
                    ));
                final_exprs.push(col(concat_of_concat_id.clone()).alias(output_name));
            }
            MapGroups { func, inputs } => {
                let func_id = agg_expr.semantic_id(schema).id;
                // No first stage aggregation for MapGroups, do all the work in the second stage.
                second_stage_aggs
                    .entry(func_id.clone())
                    .or_insert(MapGroups {
                        func: func.clone(),
                        inputs: inputs.to_vec(),
                    });
                final_exprs.push(col(output_name));
            }
            ApproxSketch(_) => {
                unimplemented!("User-facing approx_sketch aggregation is not implemented")
            }
            MergeSketch(_) => {
                unimplemented!("User-facing merge_sketch aggregation is not implemented")
            }
            ApproxPercentile(ApproxPercentileParams {
                child: e,
                percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                let sketch_id = agg_expr.semantic_id(schema).id;
                let approx_id = ApproxSketch(col(sketch_id.clone())).semantic_id(schema).id;
                first_stage_aggs
                    .entry(sketch_id.clone())
                    .or_insert(ApproxSketch(e.alias(sketch_id.clone()).clone()));
                second_stage_aggs
                    .entry(approx_id.clone())
                    .or_insert(MergeSketch(col(sketch_id.clone()).alias(approx_id.clone())));
                final_exprs.push(
                    col(approx_id.clone())
                        .sketch_percentile(percentiles.as_slice(), *force_list_output)
                        .alias(output_name),
                );
            }
        }
    }
    (first_stage_aggs, second_stage_aggs, final_exprs)
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit};
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use crate::physical_plan::PhysicalPlan;
    use crate::physical_planner::logical_to_physical;
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
        .filter(col("a").lt(lit(2)))?;
        assert_eq!(
            logical_to_physical(builder.build(), cfg.clone())?
                .clustering_spec()
                .num_partitions(),
            10
        );
        let logical_plan = builder.into_partitions(10)?.build();
        let physical_plan = logical_to_physical(logical_plan, cfg.clone())?;
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
        let physical_plan = logical_to_physical(logical_plan, cfg.clone())?;
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
}
