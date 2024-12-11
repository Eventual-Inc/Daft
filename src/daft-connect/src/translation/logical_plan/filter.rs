use eyre::bail;

use crate::translation::{to_daft_expr, to_logical_plan, Plan};

pub async fn filter(filter: spark_connect::Filter) -> eyre::Result<Plan> {
    let spark_connect::Filter { input, condition } = filter;

    let Some(input) = input else {
        bail!("input is required");
    };

    let Some(condition) = condition else {
        bail!("condition is required");
    };

    let condition = to_daft_expr(&condition)?;

    let mut plan = Box::pin(to_logical_plan(*input)).await?;
    plan.builder = plan.builder.filter(condition)?;

    Ok(plan)
}
