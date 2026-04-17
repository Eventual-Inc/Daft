use daft_local_plan::Sentinels;
#[cfg(feature = "python")]
use pyo3::{IntoPyObjectExt, Py, PyAny, PyResult, Python};

#[derive(Debug)]
pub(crate) struct ShuffleMetadata {
    pub partitions: Vec<ShufflePartitionMetadata>,
    #[allow(dead_code)]
    pub sentinels: Option<Sentinels>,
}

#[derive(Debug)]
pub(crate) struct ShufflePartitionMetadata {
    #[cfg(feature = "python")]
    pub object_ref: Option<Py<PyAny>>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl ShufflePartitionMetadata {
    pub fn new(num_rows: usize, size_bytes: usize) -> Self {
        Self {
            #[cfg(feature = "python")]
            object_ref: None,
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
