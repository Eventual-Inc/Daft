use eyre::{bail, WrapErr};
use tracing::warn;

use crate::translation::{to_logical_plan, Plan};

pub async fn sample(sample: spark_connect::Sample) -> eyre::Result<Plan> {
    let spark_connect::Sample {
        input,
        lower_bound,
        upper_bound,
        with_replacement,
        seed,
        deterministic_order,
    } = sample;

    let Some(input) = input else {
        bail!("Input is required");
    };

    let mut plan = Box::pin(to_logical_plan(*input)).await?;

    // Calculate fraction from bounds
    // todo: is this correct?
    let fraction = upper_bound - lower_bound;

    let with_replacement = with_replacement.unwrap_or(false);

    // we do not care about sign change
    let seed = seed.map(|seed| seed as u64);

    if deterministic_order {
        warn!("Deterministic order is not yet supported");
    }

    plan.builder = plan
        .builder
        .sample(fraction, with_replacement, seed)
        .wrap_err("Failed to apply sample to logical plan")?;

    Ok(plan)
}
