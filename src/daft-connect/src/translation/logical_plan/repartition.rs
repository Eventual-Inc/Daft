use eyre::{bail, ensure, WrapErr};

use crate::translation::{to_logical_plan, Plan};

pub async fn repartition(repartition: spark_connect::Repartition) -> eyre::Result<Plan> {
    let spark_connect::Repartition {
        input,
        num_partitions,
        shuffle,
    } = repartition;

    let Some(input) = input else {
        bail!("Input is required");
    };

    let num_partitions = usize::try_from(num_partitions).map_err(|_| {
        eyre::eyre!("Num partitions must be a positive integer, got {num_partitions}")
    })?;

    ensure!(
        num_partitions > 0,
        "Num partitions must be greater than 0, got {num_partitions}"
    );

    let mut plan = Box::pin(to_logical_plan(*input)).await?;

    // let's make true is default
    let shuffle = shuffle.unwrap_or(true);

    if !shuffle {
        bail!("Repartitioning without shuffling is not yet supported");
    }

    plan.builder = plan
        .builder
        .random_shuffle(Some(num_partitions))
        .wrap_err("Failed to apply random_shuffle to logical plan")?;

    Ok(plan)
}
