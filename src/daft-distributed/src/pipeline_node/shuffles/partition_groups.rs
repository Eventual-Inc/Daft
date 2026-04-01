use std::collections::{HashMap, HashSet};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;

use crate::pipeline_node::{ShufflePartitionRef, TaskOutput};

pub(crate) fn ray_partition_groups_from_outputs(
    outputs: Vec<TaskOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<PartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let TaskOutput::ShuffleWrite(output) = output else {
            return Err(DaftError::InternalError(
                "Expected Ray shuffle write task output".to_string(),
            ));
        };

        if output.partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Ray shuffle partitions, got {}",
                num_partitions,
                output.partitions.len()
            )));
        }

        for (partition_idx, partition) in output.partitions.into_iter().enumerate() {
            match partition {
                ShufflePartitionRef::Ray(partition) => {
                    if partition.num_rows() > 0 {
                        partition_groups[partition_idx].push(partition);
                    }
                }
                ShufflePartitionRef::Flight(_) => {
                    return Err(DaftError::InternalError(
                        "Expected Ray shuffle partition ref but received Flight".to_string(),
                    ));
                }
            }
        }
    }

    Ok(partition_groups)
}

pub(crate) fn flight_server_cache_mapping_from_outputs(
    outputs: Vec<TaskOutput>,
) -> DaftResult<HashMap<String, Vec<u32>>> {
    let mut server_cache_mapping: HashMap<String, HashSet<u32>> = HashMap::new();

    for output in outputs {
        let TaskOutput::ShuffleWrite(output) = output else {
            return Err(DaftError::InternalError(
                "Expected shuffle write task output for Flight shuffle write stage".to_string(),
            ));
        };

        for partition in output.partitions {
            match partition {
                ShufflePartitionRef::Ray(_) => {
                    return Err(DaftError::InternalError(
                        "Expected Flight shuffle partition ref but received Ray".to_string(),
                    ));
                }
                ShufflePartitionRef::Flight(partition) => {
                    server_cache_mapping
                        .entry(partition.server_address)
                        .or_default()
                        .insert(partition.cache_id);
                }
            }
        }
    }

    Ok(server_cache_mapping
        .into_iter()
        .map(|(server, cache_ids)| (server, cache_ids.into_iter().collect()))
        .collect())
}
