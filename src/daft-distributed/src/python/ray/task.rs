use std::{any::Any, collections::HashMap, future::Future, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::{Partition, PartitionRef};
use daft_local_plan::{
    ExecutionStats, PyFlightPartitionRef, PyLocalPhysicalPlan, SourceId, python::PyInput,
};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyResult, PyTypeInfo, Python, pyclass, pymethods};

use crate::{
    pipeline_node::MaterializedOutput,
    scheduling::{
        task::{SwordfishTask, Task, TaskContext, TaskResultHandle, TaskStatus},
        worker::WorkerId,
    },
};

#[derive(Clone)]
enum RayTaskResultInner {
    Success {
        partitions: Vec<PartitionRef>,
        stats_serialized: Vec<u8>,
    },
    WorkerDied(),
    WorkerUnavailable(),
}

#[pyclass(module = "daft.daft", name = "RayTaskResult", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct RayTaskResult {
    inner: RayTaskResultInner,
}

#[pymethods]
impl RayTaskResult {
    #[staticmethod]
    fn success_ray(ray_part_refs: Vec<RayPartitionRef>, stats_serialized: Vec<u8>) -> Self {
        Self {
            inner: RayTaskResultInner::Success {
                partitions: ray_part_refs
                    .into_iter()
                    .map(|partition| Arc::new(partition) as PartitionRef)
                    .collect(),
                stats_serialized,
            },
        }
    }

    #[staticmethod]
    fn success_flight(
        shuffle_part_refs: Vec<PyFlightPartitionRef>,
        stats_serialized: Vec<u8>,
    ) -> Self {
        Self {
            inner: RayTaskResultInner::Success {
                partitions: shuffle_part_refs
                    .into_iter()
                    .map(|partition| Arc::new(partition) as PartitionRef)
                    .collect(),
                stats_serialized,
            },
        }
    }

    #[staticmethod]
    fn worker_died() -> Self {
        Self {
            inner: RayTaskResultInner::WorkerDied(),
        }
    }

    #[staticmethod]
    fn worker_unavailable() -> Self {
        Self {
            inner: RayTaskResultInner::WorkerUnavailable(),
        }
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
    /// The ip address of the worker
    ip_address: String,
}

impl RayTaskResultHandle {
    /// Create a new TaskHandle from a Python RaySwordfishTaskHandle
    pub fn new(
        task_context: TaskContext,
        handle: Py<PyAny>,
        coroutine: Py<PyAny>,
        worker_id: WorkerId,
        ip_address: String,
    ) -> Self {
        Self {
            task_context,
            handle,
            coroutine: Some(coroutine),
            worker_id,
            ip_address,
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
        let ip_address = self.ip_address.clone();
        let worker_id = self.worker_id.clone();

        let fut = common_runtime::python::execute_python_coroutine::<_, RayTaskResult>(move |py| {
            Ok(coroutine.into_bound(py))
        });

        let task_id = self.task_context.task_id;
        async move {
            let ray_task_result = fut.await;

            match ray_task_result {
                Ok(RayTaskResult {
                    inner:
                        RayTaskResultInner::Success {
                            partitions,
                            stats_serialized,
                        },
                }) => {
                    let stats: ExecutionStats = ExecutionStats::decode(&stats_serialized);
                    let all_ray_or_flight = partitions.iter().all(|partition| {
                        partition
                            .as_any()
                            .downcast_ref::<RayPartitionRef>()
                            .is_some()
                            || partition
                                .as_any()
                                .downcast_ref::<PyFlightPartitionRef>()
                                .is_some()
                    });
                    // Task outputs are expected to be transport refs we can route in downstream nodes.
                    debug_assert!(
                        all_ray_or_flight,
                        "task output must contain ray or flight partition refs"
                    );
                    let materialized_output =
                        MaterializedOutput::new(partitions, worker_id.clone(), ip_address, task_id);

                    TaskStatus::Success {
                        result: materialized_output,
                        stats,
                    }
                }
                Ok(RayTaskResult {
                    inner: RayTaskResultInner::WorkerDied(),
                }) => TaskStatus::WorkerDied,
                Ok(RayTaskResult {
                    inner: RayTaskResultInner::WorkerUnavailable(),
                }) => TaskStatus::WorkerUnavailable,
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

#[pyclass(module = "daft.daft", name = "RayPartitionRef", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct RayPartitionRef {
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

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyAny>)> {
        Ok((
            Self::type_object(py).into(),
            (
                self.object_ref.clone_ref(py),
                self.num_rows,
                self.size_bytes,
            )
                .into_pyobject(py)?
                .into_any(),
        ))
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
    fn id(&self) -> u32 {
        self.task.task_context().task_id
    }

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

    fn inputs(&self) -> PyResult<HashMap<SourceId, PyInput>> {
        Ok(self
            .task
            .inputs()
            .iter()
            .map(|(k, v)| (*k, PyInput { inner: v.clone() }))
            .collect())
    }

    fn psets(&self) -> PyResult<HashMap<SourceId, Vec<RayPartitionRef>>> {
        let psets = self
            .task
            .psets()
            .iter()
            .map(|(k, v)| {
                (
                    *k,
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
}
