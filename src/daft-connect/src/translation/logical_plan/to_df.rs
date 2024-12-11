use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{partitioning::InMemoryPartitionSetCache, MicroPartition};
use eyre::{bail, WrapErr};

use crate::translation::to_logical_plan;

pub fn to_df(
    to_df: spark_connect::ToDf,
    pset_cache: &InMemoryPartitionSetCache<MicroPartition>,
) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::ToDf {
        input,
        column_names,
    } = to_df;

    let Some(input) = input else {
        bail!("Input is required");
    };

    let plan = to_logical_plan(*input, pset_cache)
        .wrap_err("Failed to translate relation to logical plan")?;

    let column_names: Vec<_> = column_names
        .iter()
        .map(|s| daft_dsl::col(s.as_str()))
        .collect();

    plan.select(column_names)
        .wrap_err("Failed to add columns to logical plan")
}
