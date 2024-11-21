use eyre::{bail, ensure, WrapErr};
use tracing::warn;

use crate::translation::{to_logical_plan, Plan};

pub async fn deduplicate(deduplicate: spark_connect::Deduplicate) -> eyre::Result<Plan> {
    let spark_connect::Deduplicate {
        input,
        column_names,
        all_columns_as_keys,
        within_watermark,
    } = deduplicate;

    let Some(input) = input else {
        bail!("Input is required");
    };

    if !column_names.is_empty() {
        warn!("Ignoring column_names: {column_names:?}; not yet implemented");
    }

    let all_columns_as_keys = all_columns_as_keys.unwrap_or(false);

    ensure!(
        all_columns_as_keys,
        "only implemented for all_columns_as_keys=true"
    );

    if let Some(within_watermark) = within_watermark {
        warn!("Ignoring within_watermark: {within_watermark:?}; not yet implemented");
    }

    let mut plan = Box::pin(to_logical_plan(*input)).await?;

    plan.builder = plan
        .builder
        .distinct()
        .wrap_err("Failed to apply distinct to logical plan")?;

    Ok(plan)
}
