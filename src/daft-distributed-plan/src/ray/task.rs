use std::{any::Any, collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::PyLocalPhysicalPlan;
use pyo3::{pyclass, pymethods, FromPyObject, IntoPyObject, PyObject, PyResult, Python};

use crate::{task::SwordfishTask, TASK_LOCALS};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
#[pyclass(module = "daft.daft")]
pub(crate) struct RayTaskResultHandle {
    handle: PyObject,
}

#[pymethods]
impl RayTaskResultHandle {
    #[new]
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    pub fn new(handle: PyObject) -> Self {
        Self { handle }
    }
}

impl RayTaskResultHandle {
    /// Get the result of the task, awaiting if necessary
    pub async fn get_result(&self) -> DaftResult<PartitionRef> {
        let (task_locals, handle) = Python::with_gil(|py| {
            let task_locals = TASK_LOCALS.get().unwrap().clone_ref(py);
            let handle = self.handle.clone_ref(py);
            (task_locals, handle)
        });
        println!("task_locals: {:?}", task_locals);
        let fut = async move {
            let py_awaitable = Python::with_gil(|py| {
                let coroutine = handle
                    .call_method0(py, pyo3::intern!(py, "get_result"))?
                    .into_bound(py);
                pyo3_async_runtimes::tokio::into_future(coroutine)
            })?;
            println!("py_awaitable");
            let res = py_awaitable.await;
            println!("res: {:?}", res);
            res
        };
        println!("got fut");
        let materialized_result = pyo3_async_runtimes::tokio::scope(task_locals, fut).await?;

        println!("py_result: {:?}", materialized_result);
        let materialized_result =
            Python::with_gil(|py| materialized_result.extract::<RayPartitionRef>(py))?;
        Ok(Arc::new(materialized_result))
    }
}

impl Drop for RayTaskResultHandle {
    fn drop(&mut self) {
        Python::with_gil(|py| self.handle.call_method0(py, "cancel").unwrap());
    }
}

#[derive(Debug, FromPyObject, IntoPyObject)]
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

#[pyclass(module = "daft.daft", name = "RaySwordfishTask")]
pub(crate) struct RaySwordfishTask {
    pub task: SwordfishTask,
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

    fn execution_config(&self) -> PyResult<PyDaftExecutionConfig> {
        let config = self.task.execution_config();
        Ok(PyDaftExecutionConfig { config })
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
                            let v = v.as_any().downcast_ref::<RayPartitionRef>().unwrap();
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
