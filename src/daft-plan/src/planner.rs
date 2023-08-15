use std::sync::Arc;
use std::{cmp::max, collections::HashMap};

use common_error::DaftResult;
use daft_core::count_mode::CountMode;
use daft_dsl::Expr;

use crate::logical_plan::LogicalPlan;
use crate::ops::{
    Aggregate as LogicalAggregate, Coalesce as LogicalCoalesce, Concat as LogicalConcat,
    Distinct as LogicalDistinct, Explode as LogicalExplode, Filter as LogicalFilter,
    Join as LogicalJoin, Limit as LogicalLimit, Project as LogicalProject,
    Repartition as LogicalRepartition, Sink as LogicalSink, Sort as LogicalSort, Source,
};
use crate::physical_plan::PhysicalPlan;
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::{ExternalInfo as ExternalSourceInfo, FileFormatConfig, SourceInfo};
use crate::{physical_ops::*, PartitionSpec};
use crate::{FileFormat, PartitionScheme};

#[cfg(feature = "python")]
use crate::physical_ops::InMemoryScan;

pub fn plan(logical_plan: &LogicalPlan) -> DaftResult<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Source(Source {
            schema,
            source_info,
            partition_spec,
            limit,
            filters,
        }) => match source_info.as_ref() {
            SourceInfo::ExternalInfo(
                ext_info @ ExternalSourceInfo {
                    file_format_config, ..
                },
            ) => match file_format_config.as_ref() {
                FileFormatConfig::Parquet(_) => {
                    Ok(PhysicalPlan::TabularScanParquet(TabularScanParquet::new(
                        schema.clone(),
                        ext_info.clone(),
                        partition_spec.clone(),
                        *limit,
                        filters.to_vec(),
                    )))
                }
                FileFormatConfig::Csv(_) => Ok(PhysicalPlan::TabularScanCsv(TabularScanCsv::new(
                    schema.clone(),
                    ext_info.clone(),
                    partition_spec.clone(),
                    *limit,
                    filters.to_vec(),
                ))),
                FileFormatConfig::Json(_) => {
                    Ok(PhysicalPlan::TabularScanJson(TabularScanJson::new(
                        schema.clone(),
                        ext_info.clone(),
                        partition_spec.clone(),
                        *limit,
                        filters.to_vec(),
                    )))
                }
            },
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(mem_info) => Ok(PhysicalPlan::InMemoryScan(
                InMemoryScan::new(schema.clone(), mem_info.clone(), partition_spec.clone()),
            )),
        },
        LogicalPlan::Project(LogicalProject {
            input,
            projection,
            resource_request,
            ..
        }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Project(Project::new(
                projection.clone(),
                resource_request.clone(),
                input_physical.into(),
            )))
        }
        LogicalPlan::Filter(LogicalFilter { input, predicate }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Filter(Filter::new(
                predicate.clone(),
                Arc::new(input_physical),
            )))
        }
        LogicalPlan::Limit(LogicalLimit { input, limit }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Limit(Limit::new(
                *limit,
                logical_plan.partition_spec().num_partitions,
                Arc::new(input_physical),
            )))
        }
        LogicalPlan::Explode(LogicalExplode {
            input,
            explode_exprs,
            ..
        }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Explode(Explode::new(
                explode_exprs.clone(),
                input_physical.into(),
            )))
        }
        LogicalPlan::Sort(LogicalSort {
            input,
            sort_by,
            descending,
        }) => {
            let input_physical = plan(input)?;
            let num_partitions = logical_plan.partition_spec().num_partitions;
            Ok(PhysicalPlan::Sort(Sort::new(
                sort_by.clone(),
                descending.clone(),
                num_partitions,
                input_physical.into(),
            )))
        }
        LogicalPlan::Repartition(LogicalRepartition {
            input,
            num_partitions,
            partition_by,
            scheme,
        }) => {
            let input_physical = Arc::new(plan(input)?);
            match scheme {
                PartitionScheme::Unknown => {
                    let split_op = PhysicalPlan::Split(Split::new(
                        input.partition_spec().num_partitions,
                        *num_partitions,
                        input_physical,
                    ));
                    Ok(PhysicalPlan::Flatten(Flatten::new(split_op.into())))
                }
                PartitionScheme::Random => {
                    let split_op = PhysicalPlan::FanoutRandom(FanoutRandom::new(
                        *num_partitions,
                        input_physical,
                    ));
                    Ok(PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())))
                }
                PartitionScheme::Hash => {
                    let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                        *num_partitions,
                        partition_by.clone(),
                        input_physical,
                    ));
                    Ok(PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())))
                }
                PartitionScheme::Range => unreachable!("Repartitioning by range is not supported"),
            }
        }
        LogicalPlan::Coalesce(LogicalCoalesce { input, num_to }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Coalesce(Coalesce::new(
                input_physical.into(),
                logical_plan.partition_spec().num_partitions,
                *num_to,
            )))
        }
        LogicalPlan::Distinct(LogicalDistinct { input }) => {
            let input_physical = plan(input)?;
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
            let num_partitions = logical_plan.partition_spec().num_partitions;
            if num_partitions > 1 {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    num_partitions,
                    col_exprs.clone(),
                    agg_op.into(),
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
            let input_plan = plan(input)?;

            let num_input_partitions = logical_plan.partition_spec().num_partitions;

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
                            num_input_partitions,
                            groupby.clone(),
                            first_stage_agg.into(),
                        ));
                        PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()))
                    };

                    let _second_stage_agg = PhysicalPlan::Aggregate(Aggregate::new(
                        gather_plan.into(),
                        second_stage_aggs.values().cloned().collect(),
                        groupby.clone(),
                    ));

                    PhysicalPlan::Project(Project::new(
                        final_exprs,
                        Default::default(),
                        _second_stage_agg.into(),
                    ))
                }
            };

            Ok(result_plan)
        }
        LogicalPlan::Concat(LogicalConcat { other, input }) => {
            let input_physical = plan(input)?;
            let other_physical = plan(other)?;
            Ok(PhysicalPlan::Concat(Concat::new(
                other_physical.into(),
                input_physical.into(),
            )))
        }
        LogicalPlan::Join(LogicalJoin {
            right,
            input,
            left_on,
            right_on,
            output_projection,
            join_type,
            ..
        }) => {
            let mut left_physical = plan(input)?;
            let mut right_physical = plan(right)?;
            let left_pspec = input.partition_spec();
            let right_pspec = right.partition_spec();
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
            if (num_partitions > 1 || left_pspec.num_partitions != num_partitions)
                && left_pspec != new_left_pspec
            {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    num_partitions,
                    left_on.clone(),
                    left_physical.into(),
                ));
                left_physical = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
            }
            if (num_partitions > 1 || right_pspec.num_partitions != num_partitions)
                && right_pspec != new_right_pspec
            {
                let split_op = PhysicalPlan::FanoutByHash(FanoutByHash::new(
                    num_partitions,
                    right_on.clone(),
                    right_physical.into(),
                ));
                right_physical = PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into()));
            }
            Ok(PhysicalPlan::Join(Join::new(
                right_physical.into(),
                left_on.clone(),
                right_on.clone(),
                output_projection.clone(),
                *join_type,
                left_physical.into(),
            )))
        }
        LogicalPlan::Sink(LogicalSink {
            schema,
            sink_info,
            input,
        }) => {
            let input_physical = plan(input)?;
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
