use std::{any::Any, collections::HashMap, future::Future, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::{ExecutionEngineFinalResult, PyLocalPhysicalPlan};
use pyo3::{
    Bound, Py, PyAny, PyResult, Python, pyclass, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods},
};

use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        task::{SwordfishTask, TaskContext, TaskResultHandle, TaskStatus},
        worker::WorkerId,
    },
};

#[pyclass(module = "daft.daft", name = "RayTaskResult")]
#[derive(Clone)]
pub(crate) enum RayTaskResult {
    Success(Vec<RayPartitionRef>, Vec<u8>),
    WorkerDied(),
    WorkerUnavailable(),
}

#[pymethods]
impl RayTaskResult {
    #[staticmethod]
    fn success(ray_part_refs: Vec<RayPartitionRef>, stats_serialized: Vec<u8>) -> Self {
        Self::Success(ray_part_refs, stats_serialized)
    }

    #[staticmethod]
    fn worker_died() -> Self {
        Self::WorkerDied()
    }

    #[staticmethod]
    fn worker_unavailable() -> Self {
        Self::WorkerUnavailable()
    }
}

/// TaskHandle that wraps a Python RaySwordfishTaskHandle
pub(crate) struct RayTaskResultHandle {
    task_context: TaskContext,
    /// The handle to the RaySwordfishTaskHandle
    handle: Py<PyAny>,
    /// The coroutine to await the result of the task
    coroutine: Option<Py<PyAny>>,
    /// The worker id
    worker_id: WorkerId,
}

impl RayTaskResultHandle {
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    pub fn new(
        task_context: TaskContext,
        handle: Py<PyAny>,
        coroutine: Py<PyAny>,
        worker_id: WorkerId,
    ) -> Self {
        Self {
            task_context,
            handle,
            coroutine: Some(coroutine),
            worker_id,
        }
    }
}

impl TaskResultHandle for RayTaskResultHandle {
    fn task_context(&self) -> TaskContext {
        self.task_context.clone()
    }

    /// Get the result of the task, awaiting if necessary
    fn get_result(&mut self) -> impl Future<Output = TaskStatus> + Send + 'static {
        // Create a rust future that will await the coroutine
        let coroutine = self.coroutine.take().unwrap();
        let worker_id = self.worker_id.clone();

        let fut = common_runtime::python::execute_python_coroutine::<_, RayTaskResult>(move |py| {
            Ok(coroutine.into_bound(py))
        });
        async move {
            let ray_task_result = fut.await;

            match ray_task_result {
                Ok(RayTaskResult::Success(ray_part_refs, stats_serialized)) => {
                    let stats: ExecutionEngineFinalResult =
                        ExecutionEngineFinalResult::decode(&stats_serialized);
                    let materialized_output = MaterializedOutput::new(
                        ray_part_refs
                            .into_iter()
                            .map(|ray_part_ref| Arc::new(ray_part_ref) as PartitionRef)
                            .collect(),
                        worker_id.clone(),
                    );

                    TaskStatus::Success {
                        result: materialized_output,
                        stats,
                    }
                }
                Ok(RayTaskResult::WorkerDied()) => TaskStatus::WorkerDied,
                Ok(RayTaskResult::WorkerUnavailable()) => TaskStatus::WorkerUnavailable,
                Err(e) => TaskStatus::Failed { error: e.into() },
            }
        }
    }

    fn cancel_callback(&mut self) {
        Python::attach(|py| {
            self.handle
                .call_method0(py, "cancel")
                .expect("Failed to cancel task");
        });
    }
}

#[pyclass(module = "daft.daft", name = "RayPartitionRef")]
#[derive(Debug, Clone)]
pub(crate) struct RayPartitionRef {
    pub object_refs: Vec<Arc<Py<PyAny>>>,
    #[pyo3(get)]
    pub num_rows: usize,
    #[pyo3(get)]
    pub size_bytes: usize,
}

#[pymethods]
impl RayPartitionRef {
    #[new]
    #[pyo3(signature = (object_refs=vec![], num_rows=0, size_bytes=0))]
    pub fn new(object_refs: Vec<Py<PyAny>>, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_refs: object_refs.into_iter().map(Arc::new).collect(),
            num_rows,
            size_bytes,
        }
    }

    #[getter(object_refs)]
    pub fn py_object_refs(&self, py: Python) -> Vec<Py<PyAny>> {
        self.object_refs.iter().map(|r| r.clone_ref(py)).collect()
    }

    pub fn get_object_refs(&self, py: Python) -> Vec<Py<PyAny>> {
        self.py_object_refs(py)
    }

    pub fn __setstate__(&mut self, state: &Bound<PyAny>) -> PyResult<()> {
        let dict = state.cast::<PyDict>()?;
        if let Some(object_refs) = dict.get_item("object_refs")? {
            self.object_refs = object_refs
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(Arc::new)
                .collect();
        }
        if let Some(num_rows) = dict.get_item("num_rows")? {
            self.num_rows = num_rows.extract()?;
        }
        if let Some(size_bytes) = dict.get_item("size_bytes")? {
            self.size_bytes = size_bytes.extract()?;
        }
        Ok(())
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item(
            "object_refs",
            self.object_refs
                .iter()
                .map(|r| r.clone_ref(py))
                .collect::<Vec<_>>(),
        )?;
        dict.set_item("num_rows", self.num_rows)?;
        dict.set_item("size_bytes", self.size_bytes)?;
        Ok(dict.unbind().into_any())
    }
}

impl Partition for RayPartitionRef {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn size_bytes(&self) -> usize {
        self.size_bytes
    }
    fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[pyclass(module = "daft.daft", name = "RaySwordfishTask")]
pub(crate) struct RaySwordfishTask {
    task: SwordfishTask,
}

impl RaySwordfishTask {
    pub fn new(task: SwordfishTask) -> Self {
        Self { task }
    }
}

#[pymethods]
impl RaySwordfishTask {
    fn context(&self) -> HashMap<String, String> {
        self.task.context().clone()
    }

    fn name(&self) -> String {
        self.task.name()
    }

    fn num_partitions(&self) -> usize {
        self.task.plan().output_partitions()
    }

    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.task.plan();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn psets(&self) -> PyResult<HashMap<String, Vec<RayPartitionRef>>> {
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
                                object_refs: v.object_refs.clone(),
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

    fn is_into_batches(&self) -> bool {
        self.task.is_into_batches()
    }

    fn config(&self) -> PyResult<PyDaftExecutionConfig> {
        let config = self.task.config().clone();
        Ok(PyDaftExecutionConfig { config })
    }
}
