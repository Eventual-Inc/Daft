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
    /// In non-Python (local) mode, the actual partition data is kept here
    /// instead of being put into Ray's object store.
    #[cfg(not(feature = "python"))]
    pub data: Option<std::sync::Arc<daft_micropartition::MicroPartition>>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl ShufflePartitionMetadata {
    pub fn new(num_rows: usize, size_bytes: usize) -> Self {
        Self {
            #[cfg(feature = "python")]
            object_ref: None,
            #[cfg(not(feature = "python"))]
            data: None,
            num_rows,
            size_bytes,
        }
    }

    #[cfg(feature = "python")]
    pub fn with_object_ref(object_ref: Py<PyAny>, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_ref: Some(object_ref),
            num_rows,
            size_bytes,
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn with_data(
        data: std::sync::Arc<daft_micropartition::MicroPartition>,
        num_rows: usize,
        size_bytes: usize,
    ) -> Self {
        Self {
            data: Some(data),
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
                    partition.num_rows,
                    partition.size_bytes,
                )
            })
            .collect::<Vec<_>>()
            .into_py_any(py)
    }
}
