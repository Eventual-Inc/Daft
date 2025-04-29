use std::{any::Any, collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::PyLocalPhysicalPlan;
use pyo3::{pyclass, pymethods, FromPyObject, PyObject, PyResult, Python};

use crate::scheduling::task::{SwordfishTask, SwordfishTaskResultHandle};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
#[allow(dead_code)]
pub(crate) struct RayTaskResultHandle {
    /// The handle to the task
    handle: Option<PyObject>,
    /// The task locals, i.e. the asyncio event loop
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
}

impl RayTaskResultHandle {
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    #[allow(dead_code)]
    pub fn new(handle: PyObject, task_locals: pyo3_async_runtimes::TaskLocals) -> Self {
        Self {
            handle: Some(handle),
            task_locals: Some(task_locals),
        }
    }
}

#[async_trait::async_trait]
impl SwordfishTaskResultHandle for RayTaskResultHandle {
    /// Get the result of the task, awaiting if necessary
    async fn get_result(&mut self) -> DaftResult<PartitionRef> {
        let handle = self.handle.take().unwrap();
        let coroutine = Python::with_gil(|py| {
            let coroutine = handle
                .call_method0(py, pyo3::intern!(py, "get_result"))?
                .into_bound(py);
            pyo3_async_runtimes::tokio::into_future(coroutine)
        })?;

        // await the rust future in the scope of the asyncio event loop
        let task_locals = self.task_locals.take().unwrap();
        let materialized_result = pyo3_async_runtimes::tokio::scope(task_locals, coroutine).await?;

        let ray_part_ref =
            Python::with_gil(|py| materialized_result.extract::<RayPartitionRef>(py))?;
        Ok(Arc::new(ray_part_ref))
    }
}

impl Drop for RayTaskResultHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            Python::with_gil(|py| {
                handle
                    .call_method0(py, "cancel")
                    .expect("Failed to cancel ray task")
            });
        }
    }
}

#[pyclass(module = "daft.daft", name = "RayPartitionRef")]
#[derive(Debug, FromPyObject)]
pub(crate) struct RayPartitionRef {
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

#[pyclass(module = "daft.daft", name = "RaySwordfishTask")]
pub(crate) struct RaySwordfishTask {
    task: SwordfishTask,
}

impl RaySwordfishTask {
    #[allow(dead_code)]
    pub fn new(task: SwordfishTask) -> Self {
        Self { task }
    }
}

#[pymethods]
impl RaySwordfishTask {
    fn estimated_memory_cost(&self) -> usize {
        self.task.estimated_memory_cost()
    }

    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.task.plan();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn psets(&self, py: Python) -> PyResult<HashMap<String, Vec<RayPartitionRef>>> {
        let psets = self
            .task
            .psets()
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.into_iter()
                        .map(|v| {
                            let v = v
                                .as_any()
                                .downcast_ref::<RayPartitionRef>()
                                .expect("Failed to downcast to RayPartitionRef");
                            RayPartitionRef {
                                object_ref: v.object_ref.clone_ref(py),
                                num_rows: v.num_rows,
                                size_bytes: v.size_bytes,
                            }
                        })
                        .collect(),
                )
            })
            .collect();
        Ok(psets)
    }
}
