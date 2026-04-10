use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::FlightShufflePartitionRef;

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

pub(crate) fn flight_partition_groups_from_outputs(
    outputs: Vec<TaskOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<FlightShufflePartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let TaskOutput::ShuffleWrite(output) = output else {
            return Err(DaftError::InternalError(
                "Expected shuffle write task output for Flight shuffle write stage".to_string(),
            ));
        };

        if output.partitions.is_empty() {
            continue;
        }

        if output.partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Flight shuffle partitions, got {}",
                num_partitions,
                output.partitions.len()
            )));
        }

        for (partition_idx, partition) in output.partitions.into_iter().enumerate() {
            match partition {
                ShufflePartitionRef::Ray(_) => {
                    return Err(DaftError::InternalError(
                        "Expected Flight shuffle partition ref but received Ray".to_string(),
                    ));
                }
                ShufflePartitionRef::Flight(partition) => {
                    if partition.num_rows > 0 {
                        partition_groups[partition_idx].push(partition);
                    }
                }
            }
        }
    }

    Ok(partition_groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_node::ShuffleWriteOutput;

    fn flight_ref(
        shuffle_id: u64,
        server_address: &str,
        partition_ref_id: u64,
        num_rows: usize,
    ) -> FlightShufflePartitionRef {
        FlightShufflePartitionRef {
            shuffle_id,
            server_address: server_address.to_string(),
            partition_ref_id,
            num_rows,
            size_bytes: num_rows * 10,
        }
    }

    #[test]
    fn flight_partition_groups_transpose_by_position() -> DaftResult<()> {
        let outputs = vec![
            TaskOutput::ShuffleWrite(ShuffleWriteOutput {
                partitions: vec![
                    ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-a:1234", 101, 3)),
                    ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-a:1234", 102, 0)),
                ],
            }),
            TaskOutput::ShuffleWrite(ShuffleWriteOutput {
                partitions: vec![
                    ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-b:1234", 201, 4)),
                    ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-b:1234", 202, 5)),
                ],
            }),
        ];

        let partition_groups = flight_partition_groups_from_outputs(outputs, 2)?;

        assert_eq!(
            partition_groups,
            vec![
                vec![
                    flight_ref(11, "grpc://worker-a:1234", 101, 3),
                    flight_ref(11, "grpc://worker-b:1234", 201, 4),
                ],
                vec![flight_ref(11, "grpc://worker-b:1234", 202, 5)],
            ]
        );

        Ok(())
    }

    #[test]
    fn flight_partition_groups_rejects_mismatched_lengths() {
        let outputs = vec![TaskOutput::ShuffleWrite(ShuffleWriteOutput {
            partitions: vec![ShufflePartitionRef::Flight(flight_ref(
                11,
                "grpc://worker-a:1234",
                101,
                3,
            ))],
        })];

        let err = flight_partition_groups_from_outputs(outputs, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("Expected 2 Flight shuffle partitions")
        );
    }

    #[test]
    fn flight_partition_groups_skip_zero_row_refs() -> DaftResult<()> {
        let outputs = vec![TaskOutput::ShuffleWrite(ShuffleWriteOutput {
            partitions: vec![
                ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-a:1234", 101, 0)),
                ShufflePartitionRef::Flight(flight_ref(11, "grpc://worker-a:1234", 102, 7)),
            ],
        })];

        let partition_groups = flight_partition_groups_from_outputs(outputs, 2)?;

        assert_eq!(
            partition_groups,
            vec![vec![], vec![flight_ref(11, "grpc://worker-a:1234", 102, 7)],]
        );
        Ok(())
    }

    #[test]
    fn flight_partition_groups_accept_empty_task_output_as_all_empty() -> DaftResult<()> {
        let outputs = vec![TaskOutput::ShuffleWrite(ShuffleWriteOutput {
            partitions: vec![],
        })];

        let partition_groups = flight_partition_groups_from_outputs(outputs, 2)?;

        assert_eq!(partition_groups, vec![vec![], vec![]]);
        Ok(())
    }
}
