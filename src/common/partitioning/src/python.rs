use daft_recordbatch::python::PyRecordBatch;
use pyo3::prelude::*;

use crate::PartitionRef;

/// A Python wrapper around a PartitionRef that exposes partition functionality to Python.
#[pyclass(name = "PartitionRef", module = "daft.daft", frozen)]
#[derive(Debug, Clone)]
pub struct PyPartitionRef {
    pub inner: PartitionRef,
}

impl PyPartitionRef {
    /// Create a new PyPartitionRef from a PartitionRef
    pub fn new(inner: PartitionRef) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyPartitionRef {
    /// Get the size of the partition in bytes, if available
    fn size_bytes(&self) -> PyResult<Option<usize>> {
        self.inner.size_bytes().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get size: {}", e))
        })
    }

    /// Get the number of rows in the partition
    fn num_rows(&self) -> PyResult<usize> {
        self.inner.num_rows().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to get row count: {}",
                e
            ))
        })
    }

    /// Check if the partition is empty
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Convert the partition to a list of record batches
    fn to_record_batches(&self) -> PyResult<Vec<PyRecordBatch>> {
        let batches = self.inner.to_record_batches()?;
        Ok(batches
            .iter()
            .map(|batch| PyRecordBatch::from(batch.clone()))
            .collect())
    }
}

impl From<PartitionRef> for PyPartitionRef {
    fn from(value: PartitionRef) -> Self {
        Self::new(value)
    }
}

impl From<PyPartitionRef> for PartitionRef {
    fn from(value: PyPartitionRef) -> Self {
        value.inner
    }
}
