use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::Expr;

use crate::logical_plan::LogicalPlan;
use crate::ops::{
    Aggregate as LogicalAggregate, Coalesce as LogicalCoalesce, Concat as LogicalConcat,
    Distinct as LogicalDistinct, Filter as LogicalFilter, Limit as LogicalLimit,
    Project as LogicalProject, Repartition as LogicalRepartition, Sink as LogicalSink,
    Sort as LogicalSort, Source,
};
use crate::physical_ops::*;
use crate::physical_plan::PhysicalPlan;
use crate::sink_info::{OutputFileInfo, SinkInfo};
use crate::source_info::{ExternalInfo as ExternalSourceInfo, FileFormatConfig, SourceInfo};
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
            input, projection, ..
        }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Project(Project::new(
                projection.clone(),
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
            group_by,
            input,
        }) => {
            use daft_dsl::AggExpr::*;
            let result_plan = plan(input)?;

            if !group_by.is_empty() {
                unimplemented!("{:?}", group_by);
            }

            let num_input_partitions = logical_plan.partition_spec().num_partitions;

            let result_plan = match num_input_partitions {
                1 => PhysicalPlan::Aggregate(Aggregate::new(
                    result_plan.into(),
                    aggregations.clone(),
                    vec![],
                )),
                _ => {
                    // Resolve and assign intermediate names for the aggregations.
                    let schema = logical_plan.schema();
                    let intermediate_names: Vec<daft_core::datatypes::FieldID> = aggregations
                        .iter()
                        .map(|agg_expr| agg_expr.semantic_id(&schema))
                        .collect();

                    let first_stage_aggs: Vec<daft_dsl::AggExpr> = aggregations
                        .iter()
                        .zip(intermediate_names.iter())
                        .map(|(agg_expr, field_id)| match agg_expr {
                            Count(e) => Count(e.alias(field_id.id.clone()).into()),
                            Sum(e) => Sum(e.alias(field_id.id.clone()).into()),
                            Mean(e) => Mean(e.alias(field_id.id.clone()).into()),
                            Min(e) => Min(e.alias(field_id.id.clone()).into()),
                            Max(e) => Max(e.alias(field_id.id.clone()).into()),
                            List(e) => List(e.alias(field_id.id.clone()).into()),
                            Concat(e) => Concat(e.alias(field_id.id.clone()).into()),
                        })
                        .collect();

                    let second_stage_aggs: Vec<daft_dsl::AggExpr> = intermediate_names
                        .iter()
                        .zip(schema.fields.keys())
                        .zip(aggregations.iter())
                        .map(|((field_id, original_name), agg_expr)| match agg_expr {
                            Count(_) => Count(
                                daft_dsl::Expr::Column(field_id.id.clone().into())
                                    .alias(&**original_name)
                                    .into(),
                            ),
                            Sum(_) => Sum(daft_dsl::Expr::Column(field_id.id.clone().into())
                                .alias(&**original_name)
                                .into()),
                            Mean(_) => Mean(
                                daft_dsl::Expr::Column(field_id.id.clone().into())
                                    .alias(&**original_name)
                                    .into(),
                            ),
                            Min(_) => Min(daft_dsl::Expr::Column(field_id.id.clone().into())
                                .alias(&**original_name)
                                .into()),
                            Max(_) => Max(daft_dsl::Expr::Column(field_id.id.clone().into())
                                .alias(&**original_name)
                                .into()),
                            List(_) => List(
                                daft_dsl::Expr::Column(field_id.id.clone().into())
                                    .alias(&**original_name)
                                    .into(),
                            ),
                            Concat(_) => Concat(
                                daft_dsl::Expr::Column(field_id.id.clone().into())
                                    .alias(&**original_name)
                                    .into(),
                            ),
                        })
                        .collect();

                    let result_plan = PhysicalPlan::Aggregate(Aggregate::new(
                        result_plan.into(),
                        first_stage_aggs,
                        vec![],
                    ));
                    let result_plan = PhysicalPlan::Coalesce(Coalesce::new(
                        result_plan.into(),
                        num_input_partitions,
                        1,
                    ));
                    PhysicalPlan::Aggregate(Aggregate::new(
                        result_plan.into(),
                        second_stage_aggs,
                        vec![],
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
