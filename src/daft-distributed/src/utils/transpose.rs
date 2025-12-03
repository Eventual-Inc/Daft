use common_error::DaftResult;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::pipeline_node::MaterializedOutput;

pub(crate) async fn transpose_materialized_outputs_from_stream(
    materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<MaterializedOutput>>> {
    let materialized_partitions = materialized_stream
        .map(|mat| mat.map(|mat| mat.split_into_materialized_outputs()))
        .try_collect::<Vec<_>>()
        .await?;

    Ok(transpose_materialized_outputs(
        materialized_partitions,
        num_partitions,
    ))
}

pub(crate) fn transpose_materialized_outputs_from_vec(
    materialized_partitions: Vec<MaterializedOutput>,
    num_partitions: usize,
) -> Vec<Vec<MaterializedOutput>> {
    let materialized_partitions = materialized_partitions
        .into_iter()
        .map(|mat| mat.split_into_materialized_outputs())
        .collect::<Vec<_>>();

    transpose_materialized_outputs(materialized_partitions, num_partitions)
}

/// This function takes a vector of materialized outputs (each containing multiple partitions)
/// and reorganizes them so that each output partition contains all the data for that partition
/// from all input materialized outputs.
///
/// # Arguments
/// * `materialized_partitions` - A vector of materialized outputs, each containing `num_partitions` partitions
/// * `num_partitions` - The number of partitions each materialized output should contain
///
/// # Returns
/// * A vector of partition groups, where each group contains all materialized outputs for that partition
fn transpose_materialized_outputs(
    materialized_partitions: Vec<Vec<MaterializedOutput>>,
    num_partitions: usize,
) -> Vec<Vec<MaterializedOutput>> {
    debug_assert!(
        materialized_partitions
            .iter()
            .all(|mat| mat.len() == num_partitions),
        "Expected all outputs to have {} partitions, got {}",
        num_partitions,
        materialized_partitions
            .iter()
            .map(|mat| mat.len())
            .join(", ")
    );

    let mut transposed_outputs = vec![];
    for idx in 0..num_partitions {
        let mut partition_group = vec![];
        for materialized_partition in &materialized_partitions {
            let part = &materialized_partition[idx];
            if part.num_rows() > 0 {
                partition_group.push(part.clone());
            }
        }
        transposed_outputs.push(partition_group);
    }

    assert_eq!(transposed_outputs.len(), num_partitions);
    transposed_outputs
}
