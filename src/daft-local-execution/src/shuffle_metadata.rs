#[cfg(feature = "python")]
use pyo3::{IntoPyObjectExt, Py, PyAny, PyResult, Python};

#[derive(Debug)]
pub(crate) struct ShuffleMetadata {
    pub partitions: Vec<ShufflePartitionMetadata>,
}

#[derive(Debug)]
pub(crate) struct ShufflePartitionMetadata {
    #[cfg(feature = "python")]
    pub object_ref: Option<Py<PyAny>>,
    pub partition_ref_id: Option<u64>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl ShufflePartitionMetadata {
    pub fn with_partition_ref_id(
        partition_ref_id: u64,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self {
            #[cfg(feature = "python")]
            object_ref: None,
            partition_ref_id: Some(partition_ref_id),
            num_rows,
            size_bytes,
        }
    }

    #[cfg(feature = "python")]
    pub fn with_object_ref(object_ref: Py<PyAny>, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_ref: Some(object_ref),
            partition_ref_id: None,
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
            .map(|partition| {
                (
                    partition.object_ref.as_ref().map(|obj| obj.clone_ref(py)),
                    partition.partition_ref_id,
                    partition.num_rows,
                    partition.size_bytes,
                )
            })
            .collect::<Vec<_>>()
            .into_py_any(py)
    }
}
