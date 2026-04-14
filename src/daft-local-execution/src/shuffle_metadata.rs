#[cfg(feature = "python")]
use {
    daft_local_plan::{FlightShufflePartitionRef, PyFlightShufflePartitionRef},
    pyo3::{IntoPyObjectExt, Py, PyAny, PyResult, Python, exceptions::PyRuntimeError},
};

#[derive(Debug)]
pub(crate) struct ShuffleMetadata {
    pub partitions: Vec<ShufflePartitionMetadata>,
}

#[derive(Debug)]
pub(crate) enum ShufflePartitionMetadata {
    #[cfg(feature = "python")]
    Ray {
        object_ref: Py<PyAny>,
        num_rows: usize,
        size_bytes: usize,
    },
    Flight {
        shuffle_id: u64,
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    },
}

impl ShufflePartitionMetadata {
    pub fn with_partition_ref_id(
        shuffle_id: u64,
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self::Flight {
            shuffle_id,
            partition_ref_id,
            num_rows,
            size_bytes,
        }
    }

    #[cfg(feature = "python")]
    pub fn with_object_ref(object_ref: Py<PyAny>, num_rows: usize, size_bytes: usize) -> Self {
        Self::Ray {
            object_ref,
            num_rows,
            size_bytes,
        }
    }
}

#[cfg(feature = "python")]
impl ShuffleMetadata {
    pub fn to_pyobject(
        &self,
        py: Python<'_>,
        shuffle_address: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let Some(first_partition) = self.partitions.first() else {
            return Vec::<Py<PyAny>>::new().into_py_any(py);
        };

        match first_partition {
            #[cfg(feature = "python")]
            ShufflePartitionMetadata::Ray { .. } => self
                .partitions
                .iter()
                .map(|partition| match partition {
                    #[cfg(feature = "python")]
                    ShufflePartitionMetadata::Ray {
                        object_ref,
                        num_rows,
                        size_bytes,
                    } => Ok((
                        Some(object_ref.clone_ref(py)),
                        None::<u64>,
                        *num_rows,
                        *size_bytes,
                    )),
                    ShufflePartitionMetadata::Flight { .. } => Err(PyRuntimeError::new_err(
                        "Mixed Ray and Flight shuffle metadata is not supported",
                    )),
                })
                .collect::<PyResult<Vec<_>>>()?
                .into_py_any(py),
            ShufflePartitionMetadata::Flight { .. } => {
                let shuffle_address = shuffle_address.ok_or_else(|| {
                    PyRuntimeError::new_err(
                        "Flight shuffle metadata requires a shuffle server address",
                    )
                })?;
                self.partitions
                    .iter()
                    .map(|partition| match partition {
                        ShufflePartitionMetadata::Flight {
                            shuffle_id,
                            partition_ref_id,
                            num_rows,
                            size_bytes,
                        } => Ok(PyFlightShufflePartitionRef::from(
                            FlightShufflePartitionRef {
                                shuffle_id: *shuffle_id,
                                server_address: shuffle_address.to_string(),
                                partition_ref_id: *partition_ref_id,
                                num_rows: *num_rows,
                                size_bytes: *size_bytes,
                            },
                        )),
                        #[cfg(feature = "python")]
                        ShufflePartitionMetadata::Ray { .. } => Err(PyRuntimeError::new_err(
                            "Mixed Ray and Flight shuffle metadata is not supported",
                        )),
                    })
                    .collect::<PyResult<Vec<_>>>()?
                    .into_py_any(py)
            }
        }
    }
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use pyo3::{Python, types::PyAnyMethods};

    use super::*;

    #[test]
    fn flight_metadata_to_pyobject_returns_typed_refs() {
        let metadata = ShuffleMetadata {
            partitions: vec![ShufflePartitionMetadata::Flight {
                shuffle_id: 42,
                partition_ref_id: 7,
                num_rows: 3,
                size_bytes: 30,
            }],
        };

        Python::attach(|py| {
            let pyobj = metadata
                .to_pyobject(py, Some("grpc://worker-a:1234"))
                .expect("flight metadata should convert to Python");
            let refs = pyobj
                .bind(py)
                .extract::<Vec<PyFlightShufflePartitionRef>>()
                .expect("expected typed flight refs");
            assert_eq!(refs.len(), 1);
            assert_eq!(refs[0].inner.shuffle_id, 42);
            assert_eq!(refs[0].inner.server_address, "grpc://worker-a:1234");
            assert_eq!(refs[0].inner.partition_ref_id, 7);
            assert_eq!(refs[0].inner.num_rows, 3);
            assert_eq!(refs[0].inner.size_bytes, 30);
        });
    }

    #[test]
    fn flight_metadata_to_pyobject_requires_shuffle_address() {
        let metadata = ShuffleMetadata {
            partitions: vec![ShufflePartitionMetadata::Flight {
                shuffle_id: 42,
                partition_ref_id: 7,
                num_rows: 3,
                size_bytes: 30,
            }],
        };

        Python::attach(|py| {
            let err = metadata
                .to_pyobject(py, None)
                .expect_err("flight metadata should require a shuffle server address");
            assert!(
                err.to_string()
                    .contains("Flight shuffle metadata requires a shuffle server address")
            );
        });
    }
}
