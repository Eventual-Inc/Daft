use std::{collections::HashMap, future::Future, sync::Arc};

use common_daft_config::{DaftExecutionConfig, PyDaftExecutionConfig};
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalPhysicalPlanRef, PyLocalPhysicalPlan};
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};

use super::partition::RayPartitionRef;
use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        task::{SwordfishTask, TaskResultHandle},
        worker::WorkerId,
    },
};

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
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
    /// Wrap the asyncio coroutine in a rust future.
    fn get_result(
        &mut self,
    ) -> impl Future<Output = DaftResult<Vec<MaterializedOutput>>> + Send + 'static {
        // Take the coroutine and task locals from the handle.
        let coroutine = self.coroutine.take().unwrap();
        let task_locals = self.task_locals.take().unwrap();

        // Convert the coroutine to a rust future.
        let await_coroutine = async move {
            let result = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(coroutine.into_bound(py))
            })?
            .await?;
            DaftResult::Ok(result)
        };

        let worker_id = self.worker_id.clone();

        // Await the rust future under the scope of the task locals (asyncio event loop)
        async move {
            let materialized_result =
                pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await?;
            let ray_part_refs =
                Python::with_gil(|py| materialized_result.extract::<Vec<RayPartitionRef>>(py))?;
            let materialized_outputs = ray_part_refs
                .into_iter()
                .map(|ray_part_ref| {
                    MaterializedOutput::new(
                        Arc::new(ray_part_ref) as PartitionRef,
                        worker_id.clone(),
                    )
                })
                .collect();
            Ok(materialized_outputs)
        }
    }

    // Cancel the asyncio coroutine.
    fn cancel_callback(&mut self) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.handle.call_method0(py, "cancel")?;
            Ok(())
        })
    }
}

#[pyclass(module = "daft.daft", name = "RaySwordfishTask")]
pub(crate) struct RaySwordfishTask {
    plan: LocalPhysicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    context: HashMap<String, String>,
    psets: HashMap<String, Vec<PartitionRef>>,
}

impl TryFrom<SwordfishTask> for RaySwordfishTask {
    type Error = DaftError;

    fn try_from(task: SwordfishTask) -> DaftResult<Self> {
        let plan = task.plan;
        let (merged_plan, psets) = task
            .inputs
            .merge_plan_with_input(plan, task.context.node_id.clone());
        let context = task.context.into();
        Ok(Self {
            plan: merged_plan,
            config: task.config.clone(),
            context,
            psets,
        })
    }
}

#[pymethods]
impl RaySwordfishTask {
    fn context(&self) -> HashMap<String, String> {
        self.context.clone()
    }

    fn name(&self) -> String {
        self.context["node_name"].clone()
    }

    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.plan.clone();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn psets(&self, py: Python) -> PyResult<HashMap<String, Vec<RayPartitionRef>>> {
        let psets = self
            .psets
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

    fn config(&self) -> PyResult<PyDaftExecutionConfig> {
        let config = self.config.clone();
        Ok(PyDaftExecutionConfig { config })
    }
}
