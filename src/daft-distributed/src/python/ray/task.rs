use std::{any::Any, collections::HashMap, future::Future, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_metrics::{NodeID, StatSnapshot};
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::PyLocalPhysicalPlan;
use daft_scan::python::pylib::PyScanTask;
use pyo3::{Py, PyAny, PyResult, Python, pyclass, pymethods};

use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        task::{SwordfishTask, Task, TaskContext, TaskResultHandle, TaskStatus},
        worker::WorkerId,
    },
};

#[pyclass(module = "daft.daft", name = "RayTaskResult")]
#[derive(Clone)]
pub(crate) enum RayTaskResult {
    Success(Vec<RayPartitionRef>, Option<Vec<u8>>),
    WorkerDied(),
    WorkerUnavailable(),
}

#[pymethods]
impl RayTaskResult {
    #[staticmethod]
    fn success(ray_part_refs: Vec<RayPartitionRef>, stats_serialized: Option<Vec<u8>>) -> Self {
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
                    let stats: Vec<(NodeID, StatSnapshot)> = stats_serialized
                        .map(|stats_serialized| {
                            bincode::decode_from_slice(&stats_serialized, bincode::config::legacy())
                                .expect("Failed to deserialize stats")
                                .0
                        })
                        .unwrap_or_default();
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

#[pyclass(module = "daft.daft", name = "RayPartitionRef", frozen)]
#[derive(Debug, Clone)]
pub(crate) struct RayPartitionRef {
    pub object_ref: Arc<Py<PyAny>>,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[pymethods]
impl RayPartitionRef {
    #[new]
    pub fn new(object_ref: Py<PyAny>, num_rows: usize, size_bytes: usize) -> Self {
        Self {
            object_ref: Arc::new(object_ref),
            num_rows,
            size_bytes,
        }
    }

    #[getter]
    pub fn get_object_ref(&self, py: Python) -> Py<PyAny> {
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
                                object_ref: v.object_ref.clone(),
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
        let config = self.task.config().clone();
        Ok(PyDaftExecutionConfig { config })
    }

    /// Get the last_node_id from the task context, which is used as the source_id
    fn last_node_id(&self) -> u32 {
        use crate::scheduling::task::Task;
        self.task.task_context().last_node_id
    }

    fn scan_tasks(&self) -> PyResult<HashMap<String, Vec<PyScanTask>>> {
        let scan_tasks_map = self.task.scan_tasks();
        // Convert HashMap<String, Vec<ScanTaskLikeRef>> to HashMap<String, Vec<PyScanTask>>
        let py_scan_tasks_map: HashMap<String, Vec<PyScanTask>> = scan_tasks_map
            .iter()
            .map(|(source_id, scan_tasks)| {
                let py_scan_tasks: Vec<PyScanTask> =
                    scan_tasks.iter().map(|st| PyScanTask(st.clone())).collect();
                (source_id.clone(), py_scan_tasks)
            })
            .collect();
        Ok(py_scan_tasks_map)
    }

    fn glob_paths(&self) -> PyResult<HashMap<String, Vec<String>>> {
        Ok(self.task.glob_paths().clone())
    }

    fn task_id(&self) -> u32 {
        self.task.task_context().task_id
    }
}
