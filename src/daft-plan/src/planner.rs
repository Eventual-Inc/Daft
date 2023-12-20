use std::cmp::Ordering;
use std::sync::Arc;
use std::{cmp::max, collections::HashMap};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_core::count_mode::CountMode;
use daft_dsl::Expr;
use daft_scan::file_format::FileFormatConfig;
use daft_scan::ScanExternalInfo;

use crate::logical_ops::{
    Aggregate as LogicalAggregate, Concat as LogicalConcat, Distinct as LogicalDistinct,
    Explode as LogicalExplode, Filter as LogicalFilter, Join as LogicalJoin, Limit as LogicalLimit,
    Project as LogicalProject, Repartition as LogicalRepartition, Sink as LogicalSink,
    Sort as LogicalSort, Source,
};
use crate::logical_plan::LogicalPlan;
use crate::physical_plan::PhysicalPlan;
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::{ExternalInfo as ExternalSourceInfo, LegacyExternalInfo, SourceInfo};
use crate::{physical_ops::*, PartitionSpec};
use crate::{FileFormat, PartitionScheme};

#[cfg(feature = "python")]
use crate::physical_ops::InMemoryScan;

/// Translate a logical plan to a physical plan.
pub fn plan(logical_plan: &LogicalPlan, cfg: Arc<DaftExecutionConfig>) -> DaftResult<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Source(Source {
            output_schema,
            source_info,
        }) => match source_info.as_ref() {
            SourceInfo::ExternalInfo(ExternalSourceInfo::Legacy(
                ext_info @ LegacyExternalInfo {
                    file_format_config,
                    file_infos,
                    pushdowns,
                    ..
                },
            )) => {
                let partition_spec = Arc::new(PartitionSpec::new_internal(
                    PartitionScheme::Unknown,
                    file_infos.len(),
                    None,
                ));
                match file_format_config.as_ref() {
                    FileFormatConfig::Parquet(_) => {
                        Ok(PhysicalPlan::TabularScanParquet(TabularScanParquet::new(
                            output_schema.clone(),
                            ext_info.clone(),
                            partition_spec,
                            pushdowns.clone(),
                        )))
                    }
                    FileFormatConfig::Csv(_) => {
                        Ok(PhysicalPlan::TabularScanCsv(TabularScanCsv::new(
                            output_schema.clone(),
                            ext_info.clone(),
                            partition_spec,
                            pushdowns.clone(),
                        )))
                    }
                    FileFormatConfig::Json(_) => {
                        Ok(PhysicalPlan::TabularScanJson(TabularScanJson::new(
                            output_schema.clone(),
                            ext_info.clone(),
                            partition_spec,
                            pushdowns.clone(),
                        )))
                    }
                }
            }
            SourceInfo::ExternalInfo(ExternalSourceInfo::Scan(ScanExternalInfo {
                pushdowns,
                scan_op,
                source_schema,
                ..
            })) => {
                let scan_tasks = scan_op.0.to_scan_tasks(pushdowns.clone())?;

                // Apply transformations on the ScanTasks to optimize
                let scan_tasks = daft_scan::scan_task_iters::merge_by_sizes(
                    scan_tasks,
                    cfg.merge_scan_tasks_min_size_bytes,
                    cfg.merge_scan_tasks_max_size_bytes,
                );
                let scan_tasks = scan_tasks.collect::<DaftResult<Vec<_>>>()?;                
                if scan_tasks.is_empty() {
                    let partition_spec = Arc::new(PartitionSpec::new_internal(
                        PartitionScheme::Unknown,
                        1,
                        None,
                    ));

                    Ok(PhysicalPlan::EmptyScan(EmptyScan::new(
                        source_schema.clone(),
                        partition_spec,
                    )))
                } else {
                    let partition_spec = Arc::new(PartitionSpec::new_internal(
                        PartitionScheme::Unknown,
                        scan_tasks.len(),
                        None,
                    ));

                    Ok(PhysicalPlan::TabularScan(TabularScan::new(
                        scan_tasks,
                        partition_spec,
                    )))
                }


            }
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(mem_info) => {
                let scan = PhysicalPlan::InMemoryScan(InMemoryScan::new(
                    mem_info.source_schema.clone(),
                    mem_info.clone(),
                    PartitionSpec::new(PartitionScheme::Unknown, mem_info.num_partitions, None)
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
            let partition_spec = input_physical.partition_spec().clone();
            Ok(PhysicalPlan::Project(Project::try_new(
                input_physical.into(),
                projection.clone(),
                resource_request.clone(),
                partition_spec,
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
            let num_partitions = input_physical.partition_spec().num_partitions;
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
            Ok(PhysicalPlan::Explode(Explode::new(
                input_physical.into(),
                to_explode.clone(),
            )))
        }
        LogicalPlan::Sort(LogicalSort {
            input,
            sort_by,
            descending,
        }) => {
            let input_physical = plan(input, cfg)?;
            let num_partitions = input_physical.partition_spec().num_partitions;
            Ok(PhysicalPlan::Sort(Sort::new(
                input_physical.into(),
                sort_by.clone(),
                descending.clone(),
                num_partitions,
            )))
        }
        LogicalPlan::Repartition(LogicalRepartition {
            input,
            num_partitions,
            partition_by,
            scheme,
        }) => {
            // Below partition-dropping optimization assumes we are NOT repartitioning using a range partitioning scheme.
            // A range repartitioning of an existing range-partitioned DataFrame is only redundant if the partition boundaries
            // are consistent, which is only the case if boundary sampling is deterministic within a query.
            assert!(!matches!(scheme, PartitionScheme::Range));

            let input_physical = plan(input, cfg)?;
            let input_partition_spec = input_physical.partition_spec();
            let input_num_partitions = input_partition_spec.num_partitions;
            let num_partitions = num_partitions.unwrap_or(input_num_partitions);
            // Partition spec after repartitioning.
            let repartitioned_partition_spec = PartitionSpec::new_internal(
                scheme.clone(),
                num_partitions,
                Some(partition_by.clone()),
            );
            // Drop the repartition if the output of the repartition would yield the same partitioning as the input.
            if (input_num_partitions == 1 && num_partitions == 1)
                // Simple split/coalesce repartition to the same # of partitions is a no-op, no matter the upstream partitioning scheme.
                || (num_partitions == input_num_partitions && matches!(scheme, PartitionScheme::Unknown))
                // Repartitioning to the same partition spec as the input is always a no-op.
                || (&repartitioned_partition_spec == input_partition_spec.as_ref())
            {
                return Ok(input_physical);
            }
            let input_physical = Arc::new(input_physical);
            let repartitioned_plan = match scheme {
                PartitionScheme::Unknown => match num_partitions.cmp(&input_num_partitions) {
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
                },
                PartitionScheme::Random => {
                    let split_op = PhysicalPlan::FanoutRandom(FanoutRandom::new(
                        input_physical,
                        num_partitions,
                    ));
                    PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                }
                PartitionScheme::Hash => {
                    let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                        input_physical,
                        num_partitions,
                        partition_by.clone(),
                    ));
                    PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                }
                PartitionScheme::Range => unreachable!("Repartitioning by range is not supported"),
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
            let num_partitions = agg_op.partition_spec().num_partitions;
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
        LogicalPlan::Aggregate(LogicalAggregate {
            aggregations,
            groupby,
            input,
            ..
        }) => {
            use daft_dsl::AggExpr::{self, *};
            use daft_dsl::Expr::Column;
            let input_plan = plan(input, cfg)?;

            let num_input_partitions = input_plan.partition_spec().num_partitions;

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
                        }
                    }

                    let first_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        input_plan.into(),
                        first_stage_aggs.values().cloned().collect(),
                        groupby.clone(),
                    ));
                    let gather_plan = if groupby.is_empty() {
                        PhysicalPlan::Coalesce(Coalesce::new(
                            first_stage_agg.into(),
                            num_input_partitions,
                            1,
                        ))
                    } else {
                        let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                            first_stage_agg.into(),
                            num_input_partitions,
                            groupby.clone(),
                        ));
                        PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                    };

                    let second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan.into(),
                        second_stage_aggs.values().cloned().collect(),
                        groupby.clone(),
                    ));

                    let partition_spec = second_stage_agg.partition_spec().clone();
                    PhysicalPlan::Project(Project::try_new(
                        second_stage_agg.into(),
                        final_exprs,
                        Default::default(),
                        partition_spec,
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
            ..
        }) => {
            let mut left_physical = plan(left, cfg.clone())?;
            let mut right_physical = plan(right, cfg.clone())?;

            let left_pspec = left_physical.partition_spec();
            let right_pspec = right_physical.partition_spec();
            let num_partitions = max(left_pspec.num_partitions, right_pspec.num_partitions);
            let new_left_pspec = Arc::new(PartitionSpec::new_internal(
                PartitionScheme::Hash,
                num_partitions,
                Some(left_on.clone()),
            ));
            let new_right_pspec = Arc::new(PartitionSpec::new_internal(
                PartitionScheme::Hash,
                num_partitions,
                Some(right_on.clone()),
            ));

            let is_left_partitioned = left_pspec == new_left_pspec;
            let is_right_partitioned = right_pspec == new_right_pspec;

            // If either the left or right side of the join are very small tables, perform a broadcast join with the
            // entire smaller table broadcast to each of the partitions of the larger table.

            // Ensure that the left side of the join is the smaller side.
            let (smaller_size_bytes, do_swap) = match (
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
            let is_larger_partitioned = if do_swap {
                is_left_partitioned
            } else {
                is_right_partitioned
            };
            // If larger table is not already partitioned on the join key AND the smaller table is under broadcast size threshold, use broadcast join.
            if !is_larger_partitioned && let Some(smaller_size_bytes) = smaller_size_bytes && smaller_size_bytes <= cfg.broadcast_join_size_bytes_threshold {
                if do_swap {
                    // These will get swapped back when doing the actual local joins.
                    (left_physical, right_physical) = (right_physical, left_physical);
                }
                return Ok(PhysicalPlan::BroadcastJoin(BroadcastJoin::new(left_physical.into(), right_physical.into(), left_on.clone(), right_on.clone(), *join_type, do_swap)));
            }
            if (num_partitions > 1 || left_pspec.num_partitions != num_partitions)
                && !is_left_partitioned
            {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    left_physical.into(),
                    num_partitions,
                    left_on.clone(),
                ));
                left_physical = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
            }
            if (num_partitions > 1 || right_pspec.num_partitions != num_partitions)
                && !is_right_partitioned
            {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    right_physical.into(),
                    num_partitions,
                    right_on.clone(),
                ));
                right_physical = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
            }
            Ok(PhysicalPlan::HashJoin(HashJoin::new(
                left_physical.into(),
                right_physical.into(),
                left_on.clone(),
                right_on.clone(),
                *join_type,
            )))
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
    use crate::{test::dummy_scan_node, PartitionScheme};

    /// Tests that planner drops a simple Repartition (e.g. df.into_partitions()) the child already has the desired number of partitions.
    ///
    /// Repartition-upstream_op -> upstream_op
    #[test]
    fn repartition_dropped_redundant_into_partitions() -> DaftResult<()> {
        let cfg: Arc<DaftExecutionConfig> = DaftExecutionConfig::default().into();
        // dummy_scan_node() will create the default PartitionSpec, which only has a single partition.
        let builder = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .repartition(Some(10), vec![], PartitionScheme::Unknown)?
        .filter(col("a").lt(&lit(2)))?;
        assert_eq!(
            plan(builder.build().as_ref(), cfg.clone())?
                .partition_spec()
                .num_partitions,
            10
        );
        let logical_plan = builder
            .repartition(Some(10), vec![], PartitionScheme::Unknown)?
            .build();
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
        // dummy_scan_node() will create the default PartitionSpec, which only has a single partition.
        let builder = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        assert_eq!(
            plan(builder.build().as_ref(), cfg.clone())?
                .partition_spec()
                .num_partitions,
            1
        );
        let logical_plan = builder
            .repartition(Some(1), vec![col("a")], PartitionScheme::Hash)?
            .build();
        let physical_plan = plan(logical_plan.as_ref(), cfg.clone())?;
        assert_matches!(physical_plan, PhysicalPlan::TabularScanJson(_));
        Ok(())
    }

    /// Tests that planner drops a Repartition if both the Repartition and the child have the same partition spec.
    ///
    /// Repartition-upstream_op -> upstream_op
    #[test]
    fn repartition_dropped_same_partition_spec() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .repartition(Some(10), vec![col("a")], PartitionScheme::Hash)?
        .filter(col("a").lt(&lit(2)))?
        .repartition(Some(10), vec![col("a")], PartitionScheme::Hash)?
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
    fn repartition_dropped_same_partition_spec_agg() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .repartition(Some(10), vec![col("a")], PartitionScheme::Hash)?
        .aggregate(
            vec![Expr::Agg(AggExpr::Sum(col("a").into()))],
            vec![col("b")],
        )?
        .repartition(Some(10), vec![col("b")], PartitionScheme::Hash)?
        .build();
        let physical_plan = plan(logical_plan.as_ref(), cfg)?;
        // Check that the last repartition was dropped (the last op should be a projection for a multi-partition aggregation).
        assert_matches!(physical_plan, PhysicalPlan::Project(_));
        Ok(())
    }
}
