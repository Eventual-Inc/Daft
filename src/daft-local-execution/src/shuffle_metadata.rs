#[cfg(feature = "python")]
use pyo3::{IntoPyObjectExt, Py, PyAny, PyResult, Python};

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
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    },
}

impl ShufflePartitionMetadata {
    pub fn with_partition_ref_id(
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self::Flight {
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
    pub fn to_pyobject(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.partitions
            .iter()
            .map(|partition| match partition {
                #[cfg(feature = "python")]
                ShufflePartitionMetadata::Ray {
                    object_ref,
                    num_rows,
                    size_bytes,
                } => (Some(object_ref.clone_ref(py)), None, *num_rows, *size_bytes),
                ShufflePartitionMetadata::Flight {
                    partition_ref_id,
                    num_rows,
                    size_bytes,
                } => (None, Some(*partition_ref_id), *num_rows, *size_bytes),
            })
            .collect::<Vec<_>>()
            .into_py_any(py)
    }
}
