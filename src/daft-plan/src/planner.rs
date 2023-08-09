use std::sync::Arc;

use common_error::DaftResult;

use crate::logical_plan::LogicalPlan;
use crate::ops::{
    Aggregate as LogicalAggregate, Filter as LogicalFilter, Limit as LogicalLimit,
    Repartition as LogicalRepartition, Sort as LogicalSort, Source,
};
use crate::physical_ops::{
    Aggregate, Filter, Limit, ReduceMerge, Sort, Split, SplitByHash, SplitRandom, TabularScanCsv,
    TabularScanJson, TabularScanParquet,
};
use crate::physical_plan::PhysicalPlan;
use crate::source_info::{ExternalInfo, FileFormatConfig, SourceInfo};
use crate::PartitionScheme;

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
                PartitionScheme::Unknown => Ok(PhysicalPlan::Split(Split::new(
                    input.partition_spec().num_partitions,
                    *num_partitions,
                    input_physical,
                ))),
                PartitionScheme::Random => {
                    let split_op = PhysicalPlan::SplitRandom(SplitRandom::new(
                        *num_partitions,
                        input_physical,
                    ));
                    Ok(PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())))
                }
                PartitionScheme::Hash => {
                    let split_op = PhysicalPlan::SplitByHash(SplitByHash::new(
                        *num_partitions,
                        partition_by.clone(),
                        input_physical,
                    ));
                    Ok(PhysicalPlan::ReduceMerge(ReduceMerge::new(split_op.into())))
                }
                PartitionScheme::Range => unreachable!("Repartitioning by range is not supported"),
            }
        }
        LogicalPlan::Aggregate(LogicalAggregate {
            schema,
            aggregations,
            group_by,
            input,
        }) => {
            let input_physical = plan(input)?;
            Ok(PhysicalPlan::Aggregate(Aggregate::new(
                input_physical.into(),
                aggregations.clone(),
                group_by.clone(),
                schema.clone(),
            )))
        }
    }
}
