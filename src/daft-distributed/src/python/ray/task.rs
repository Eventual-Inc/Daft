use std::{any::Any, collections::HashMap, future::Future, sync::Arc};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::PyLocalPhysicalPlan;
use pyo3::{pyclass, pymethods, FromPyObject, PyObject, PyResult, Python};

use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        task::{SwordfishTask, TaskResultHandle},
        worker::WorkerId,
    },
};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
#[allow(dead_code)]
pub(crate) struct RayTaskResultHandle {
    /// The handle to the RaySwordfishTaskHandle
    handle: PyObject,
    /// The coroutine to await the result of the task
    coroutine: Option<PyObject>,
    /// The task locals, i.e. the asyncio event loop
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
    /// The worker id
    worker_id: WorkerId,
}

impl RayTaskResultHandle {
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    #[allow(dead_code)]
    pub fn new(
        handle: PyObject,
        coroutine: PyObject,
        task_locals: pyo3_async_runtimes::TaskLocals,
        worker_id: WorkerId,
    ) -> Self {
        Self {
            handle,
            coroutine: Some(coroutine),
            task_locals: Some(task_locals),
            worker_id,
        }
    }
}

impl TaskResultHandle for RayTaskResultHandle {
    /// Get the result of the task, awaiting if necessary
    fn get_result(
        &mut self,
    ) -> impl Future<Output = DaftResult<Vec<MaterializedOutput>>> + Send + 'static {
        // Create a rust future that will await the coroutine
        let coroutine = self.coroutine.take().unwrap();
        let task_locals = self.task_locals.take().unwrap();

        let await_coroutine = async move {
            let result = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(coroutine.into_bound(py))
            })?
            .await?;
            DaftResult::Ok(result)
        };

        let worker_id = self.worker_id.clone();
        async move {
            let materialized_result =
                pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await?;
            let ray_part_ref =
                Python::with_gil(|py| materialized_result.extract::<RayPartitionRef>(py))?;
            let partition_ref = Arc::new(ray_part_ref) as PartitionRef;
            Ok(vec![MaterializedOutput::new(partition_ref, worker_id)])
        }
    }

    fn cancel_callback(&mut self) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.handle.call_method0(py, "cancel")?;
            Ok(())
        })
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
    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.task.plan();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn psets(&self, py: Python) -> PyResult<HashMap<String, Vec<RayPartitionRef>>> {
        let psets = self
            .task
            .psets()
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
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
