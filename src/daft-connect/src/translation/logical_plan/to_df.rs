use eyre::{bail, WrapErr};
use daft_logical_plan::LogicalPlanBuilder;
use crate::translation::to_logical_plan;

pub fn to_df(to_df: spark_connect::ToDf) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::ToDf {
        input,
        column_names,
    } = to_df;

    let Some(input) = input else {
        bail!("Input is required")
    };

    let plan = to_logical_plan(*input)
        .wrap_err_with(|| format!("Failed to translate relation to logical plan: {input:?}"))?;

    let column_names: Vec<_> = column_names
        .iter()
        .map(|name| daft_dsl::col(name))
        .collect();

    let plan = plan.with_columns(column_names)?

    Ok(plan)
}
