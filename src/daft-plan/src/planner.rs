use std::sync::Arc;

use common_error::DaftResult;

use crate::logical_plan::LogicalPlan;
use crate::ops::{
    Aggregate as LogicalAggregate, Filter as LogicalFilter, Limit as LogicalLimit, Source,
};
use crate::physical_ops::*;
use crate::physical_plan::PhysicalPlan;
use crate::source_info::{ExternalInfo, FileFormatConfig, SourceInfo};

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
                ext_info @ ExternalInfo {
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
                        logical_plan.partition_spec().num_partitions,
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
    }
}
