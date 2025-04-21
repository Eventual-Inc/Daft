use std::{any::Any, sync::Arc};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use pyo3::{pyclass, pymethods, FromPyObject, PyObject, Python};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
#[pyclass(module = "daft.daft")]
pub(crate) struct RayTaskHandle {
    handle: PyObject,
}

#[pymethods]
impl RayTaskHandle {
    #[new]
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    pub fn new(handle: PyObject) -> Self {
        Self { handle }
    }
}

impl RayTaskHandle {
    /// Get the result of the task, awaiting if necessary
    pub async fn get_result(&self) -> DaftResult<Vec<PartitionRef>> {
        let py_result = Python::with_gil(|py| {
            pyo3_async_runtimes::tokio::into_future(
                self.handle
                    .call_method1(py, "get_result", ())?
                    .into_bound(py),
            )
        })?
        .await?;
        let materialized_result =
            Python::with_gil(|py| py_result.extract::<Vec<RayPartitionRef>>(py))?;
        Ok(materialized_result
            .into_iter()
            .map(|p| Arc::new(p) as PartitionRef)
            .collect())
    }
}

#[derive(Debug, FromPyObject)]
pub struct RayPartitionRef {
    pub object_ref: PyObject,
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl Partition for RayPartitionRef {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        Ok(Some(self.size_bytes))
    }
    fn num_rows(&self) -> DaftResult<usize> {
        Ok(self.num_rows)
    }
}
