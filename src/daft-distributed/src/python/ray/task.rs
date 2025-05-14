use std::{any::Any, collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::PyLocalPhysicalPlan;
use pyo3::{pyclass, pymethods, FromPyObject, PyObject, PyResult, Python};

use crate::scheduling::task::{SwordfishTask, SwordfishTaskResultHandle, Task};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
#[allow(dead_code)]
pub(crate) struct RayTaskResultHandle {
    /// The handle to the task
    handle: PyObject,
    /// The task locals, i.e. the asyncio event loop
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
}

impl RayTaskResultHandle {
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    #[allow(dead_code)]
    pub fn new(handle: PyObject, task_locals: pyo3_async_runtimes::TaskLocals) -> Self {
        Self {
            handle,
            task_locals: Some(task_locals),
        }
    }
}

#[async_trait::async_trait]
impl SwordfishTaskResultHandle for RayTaskResultHandle {
    /// Get the result of the task, awaiting if necessary
    async fn get_result(&mut self) -> DaftResult<Vec<PartitionRef>> {
        // Create a coroutine that will await the result of the task
        let coroutine = Python::with_gil(|py| {
            self.handle
                .call_method0(py, pyo3::intern!(py, "get_result"))
                .expect("Failed to get result from RayTaskResultHandle")
        });

        // Create a rust future that will await the coroutine
        let await_coroutine = async move {
            let result = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(coroutine.into_bound(py))
            })?;
            DaftResult::Ok(result.await)
        };

        // await the rust future in the scope of the asyncio event loop
        let task_locals = self.task_locals.take().unwrap();
        let materialized_result =
            pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await??;
        let ray_part_refs =
            Python::with_gil(|py| materialized_result.extract::<Vec<RayPartitionRef>>(py))?;
        Ok(ray_part_refs
            .into_iter()
            .map(|r| Arc::new(r) as PartitionRef)
            .collect::<Vec<PartitionRef>>())
    }
}

impl Drop for RayTaskResultHandle {
    fn drop(&mut self) {
        Python::with_gil(|py| {
            self.handle
                .call_method0(py, "cancel")
                .expect("Failed to cancel ray task")
        });
    }
}

#[pyclass(module = "daft.daft", name = "RayPartitionRef")]
#[derive(Debug, FromPyObject)]
pub(crate) struct RayPartitionRef {
    pub object_ref: PyObject,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[pymethods]
impl RayPartitionRef {
    #[new]
    pub fn new(object_ref: PyObject, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_ref,
            num_rows,
            size_bytes,
        }
    }

    #[getter]
    pub fn get_object_ref(&self, py: Python) -> PyObject {
        self.object_ref.clone_ref(py)
    }

    #[getter]
    pub fn get_num_rows(&self) -> usize {
        self.num_rows
    }

    #[getter]
    pub fn get_size_bytes(&self) -> usize {
        self.size_bytes
    }
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
    task: Box<SwordfishTask>,
}

impl RaySwordfishTask {
    #[allow(dead_code)]
    pub fn new(task: Box<SwordfishTask>) -> Self {
        Self { task }
    }
}

#[pymethods]
impl RaySwordfishTask {
    fn task_id(&self) -> String {
        self.task.task_id().to_string()
    }

    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.task.plan();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn config(&self) -> PyResult<PyDaftExecutionConfig> {
        let config = self.task.config();
        Ok(PyDaftExecutionConfig {
            config: config.clone(),
        })
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
