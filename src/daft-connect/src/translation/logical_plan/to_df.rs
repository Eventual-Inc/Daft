use eyre::{bail, WrapErr};

use crate::translation::{logical_plan::Plan, to_logical_plan};

pub fn to_df(to_df: spark_connect::ToDf) -> eyre::Result<Plan> {
    let spark_connect::ToDf {
        input,
        column_names,
    } = to_df;

    let Some(input) = input else {
        bail!("Input is required");
    };

    let mut plan =
        to_logical_plan(*input).wrap_err("Failed to translate relation to logical plan")?;

    let column_names: Vec<_> = column_names
        .iter()
        .map(|s| daft_dsl::col(s.as_str()))
        .collect();

    plan.builder = plan
        .builder
        .select(column_names)
        .wrap_err("Failed to add columns to logical plan")?;

    Ok(plan)
}
