use std::sync::Arc;

use common_error::DaftResult;

use crate::logical_plan::LogicalPlan;
use crate::ops::{
    Aggregate as LogicalAggregate, Filter as LogicalFilter, Limit as LogicalLimit, Source,
};
use crate::physical_ops::{Aggregate, Filter, Limit, TabularScanParquet};
use crate::physical_plan::PhysicalPlan;
use crate::source_info::FileFormatConfig;

pub fn plan(logical_plan: &LogicalPlan) -> DaftResult<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Source(Source {
            schema,
            source_info,
            partition_spec,
            limit,
            filters,
        }) => match *source_info.file_format_config {
            FileFormatConfig::Parquet(_) => {
                Ok(PhysicalPlan::TabularScanParquet(TabularScanParquet::new(
                    schema.clone(),
                    source_info.clone(),
                    partition_spec.clone(),
                    *limit,
                    filters.to_vec(),
                )))
            }
            _ => todo!("format not implemented"),
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
