use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::FlightShufflePartitionRef;
#[cfg(feature = "python")]
use daft_local_plan::PyFlightShufflePartitionRef;

use crate::pipeline_node::MaterializedOutput;
#[cfg(feature = "python")]
use crate::python::ray::RayPartitionRef;

pub(crate) fn ray_partition_groups_from_outputs(
    outputs: Vec<MaterializedOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<PartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let (partitions, _, _) = output.into_inner();

        if partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Ray shuffle partitions, got {}",
                num_partitions,
                partitions.len()
            )));
        }

        for (partition_idx, partition) in partitions.into_iter().enumerate() {
            #[cfg(feature = "python")]
            if partition
                .as_any()
                .downcast_ref::<PyFlightShufflePartitionRef>()
                .is_some()
            {
                return Err(DaftError::InternalError(
                    "Expected Ray shuffle partition ref but received Flight".to_string(),
                ));
            }
            #[cfg(feature = "python")]
            {
                // Ray shuffle readers require Ray partition refs at this boundary.
                debug_assert!(
                    partition
                        .as_any()
                        .downcast_ref::<RayPartitionRef>()
                        .is_some(),
                    "ray shuffle output must contain RayPartitionRef entries"
                );
            }

            if partition.num_rows() > 0 {
                partition_groups[partition_idx].push(partition);
            }
        }
    }

    Ok(partition_groups)
}

pub(crate) fn flight_partition_groups_from_outputs(
    outputs: Vec<MaterializedOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<FlightShufflePartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let (partitions, _, _) = output.into_inner();

        if partitions.is_empty() {
            continue;
        }

        if partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Flight shuffle partitions, got {}",
                num_partitions,
                partitions.len()
            )));
        }

        for (partition_idx, partition) in partitions.into_iter().enumerate() {
            #[cfg(feature = "python")]
            {
                // Flight shuffle readers require FlightShufflePartitionRef entries at this boundary.
                let Some(flight_partition) = partition
                    .as_any()
                    .downcast_ref::<PyFlightShufflePartitionRef>()
                else {
                    return Err(DaftError::InternalError(
                        "Expected Flight shuffle partition ref but received Ray".to_string(),
                    ));
                };
                debug_assert!(
                    partition
                        .as_any()
                        .downcast_ref::<RayPartitionRef>()
                        .is_none(),
                    "flight shuffle output must not contain RayPartitionRef entries"
                );
                if flight_partition.inner.num_rows > 0 {
                    partition_groups[partition_idx].push(flight_partition.inner.clone());
                }
            }
            #[cfg(not(feature = "python"))]
            {
                let _ = partition;
                return Err(DaftError::InternalError(
                    "Flight shuffle partition refs require python feature".to_string(),
                ));
            }
        }
    }

    Ok(partition_groups)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_partitioning::PartitionRef;
    #[cfg(feature = "python")]
    use daft_local_plan::PyFlightShufflePartitionRef;

    use super::*;

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
        #[cfg(not(feature = "python"))]
        {
            return Ok(());
        }
        #[cfg(feature = "python")]
        let outputs = vec![
            crate::pipeline_node::MaterializedOutput::new(
                vec![
                    Arc::new(PyFlightShufflePartitionRef {
                        inner: flight_ref(11, "grpc://worker-a:1234", 101, 3),
                    }) as PartitionRef,
                    Arc::new(PyFlightShufflePartitionRef {
                        inner: flight_ref(11, "grpc://worker-a:1234", 102, 0),
                    }) as PartitionRef,
                ],
                "".into(),
                String::new(),
                0,
            ),
            crate::pipeline_node::MaterializedOutput::new(
                vec![
                    Arc::new(PyFlightShufflePartitionRef {
                        inner: flight_ref(11, "grpc://worker-b:1234", 201, 4),
                    }) as PartitionRef,
                    Arc::new(PyFlightShufflePartitionRef {
                        inner: flight_ref(11, "grpc://worker-b:1234", 202, 5),
                    }) as PartitionRef,
                ],
                "".into(),
                String::new(),
                1,
            ),
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
        #[cfg(not(feature = "python"))]
        {
            return;
        }
        #[cfg(feature = "python")]
        let outputs = vec![crate::pipeline_node::MaterializedOutput::new(
            vec![Arc::new(PyFlightShufflePartitionRef {
                inner: flight_ref(11, "grpc://worker-a:1234", 101, 3),
            }) as PartitionRef],
            "".into(),
            String::new(),
            0,
        )];

        let err = flight_partition_groups_from_outputs(outputs, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("Expected 2 Flight shuffle partitions")
        );
    }

    #[test]
    fn flight_partition_groups_skip_zero_row_refs() -> DaftResult<()> {
        #[cfg(not(feature = "python"))]
        {
            return Ok(());
        }
        #[cfg(feature = "python")]
        let outputs = vec![crate::pipeline_node::MaterializedOutput::new(
            vec![
                Arc::new(PyFlightShufflePartitionRef {
                    inner: flight_ref(11, "grpc://worker-a:1234", 101, 0),
                }) as PartitionRef,
                Arc::new(PyFlightShufflePartitionRef {
                    inner: flight_ref(11, "grpc://worker-a:1234", 102, 7),
                }) as PartitionRef,
            ],
            "".into(),
            String::new(),
            0,
        )];

        let partition_groups = flight_partition_groups_from_outputs(outputs, 2)?;

        assert_eq!(
            partition_groups,
            vec![vec![], vec![flight_ref(11, "grpc://worker-a:1234", 102, 7)],]
        );
        Ok(())
    }

    #[test]
    fn flight_partition_groups_accept_empty_task_output_as_all_empty() -> DaftResult<()> {
        let outputs = vec![crate::pipeline_node::MaterializedOutput::new(
            vec![],
            "".into(),
            String::new(),
            0,
        )];

        let partition_groups = flight_partition_groups_from_outputs(outputs, 2)?;

        assert_eq!(partition_groups, vec![vec![], vec![]]);
        Ok(())
    }
}
