mod dashboard;
mod progress_bar;
pub mod ray;
use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_display::{DisplayLevel, tree::TreeDisplay};
use common_metrics::Meter;
use common_partitioning::Partition;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_local_plan::python::PyExecutionStats;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_partition_refs::{FlightPartitionRef, PyFlightPartitionRef, RayPartitionRef};
use dashboard::DashboardStatisticsSubscriber;
use futures::StreamExt;
use progress_bar::FlotillaProgressBar;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use ray::{RaySwordfishTask, RaySwordfishWorker, RayWorkerManager};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    pipeline_node::{
        logical_plan_to_pipeline_node, viz_distributed_pipeline_ascii,
        viz_distributed_pipeline_mermaid,
    },
    plan::{DistributedPhysicalPlan, PlanConfig, PlanResultStream, PlanRunner},
    python::ray::RayTaskResult,
    statistics::{
        StatisticsManager, StatisticsManagerRef, StatisticsSubscriber,
        task_lifecycle::{TaskLifecycleEventSubscriber, task_events_enabled},
    },
};

#[pyclass(frozen)]
struct PythonPartitionRefStream {
    inner: Arc<Mutex<PlanResultStream>>,
    statistics_manager: StatisticsManagerRef,
    /// Shuffle dirs to clean up once Python has finished fetching every ref
    /// the plan produced. Consumed (taken) by the first `finish()` call.
    /// Deferring cleanup to `finish()` avoids racing with in-flight
    /// `FlightPartitionRef::fetch` calls still resolving on the driver.
    shuffle_dirs: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<Vec<String>>>>>,
}

#[pymethods]
impl PythonPartitionRefStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let inner = self.inner.clone();
        // future into py requires that the future is Send + 'static, so we wrap the inner in an Arc<Mutex<>>
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = {
                let mut inner = inner.lock().await;
                inner.next().await
            };
            let ref_value = match next {
                None => None,
                Some(result) => {
                    let part = result?;
                    // Transport only: yield whichever ref type the runtime
                    // produced. Resolution (ray.get / FlightPartitionRef.fetch)
                    // happens in Python-side consumers, so data stays on the
                    // producing node until someone actually needs the bytes.
                    let obj = Python::attach(|py| -> PyResult<Py<PyAny>> {
                        if let Some(r) = part.as_any().downcast_ref::<RayPartitionRef>() {
                            Ok(r.clone().into_pyobject(py)?.into_any().unbind())
                        } else if let Some(f) = part.as_any().downcast_ref::<FlightPartitionRef>() {
                            Ok(PyFlightPartitionRef::from(f.clone())
                                .into_pyobject(py)?
                                .into_any()
                                .unbind())
                        } else {
                            Err(PyRuntimeError::new_err(
                                "PythonPartitionRefStream received unknown partition ref type",
                            ))
                        }
                    })?;
                    Some(obj)
                }
            };
            Ok(ref_value)
        })
    }

    fn finish(&self) -> PyResult<PyExecutionStats> {
        // Python signals end-of-consumption here, which is the safe point to
        // clean up any Flight shuffle dirs: every `FlightPartitionRef::fetch`
        // issued by the driver has returned, so no reader on the flight server
        // is about to touch these files. Fire-and-forget: spawn cleanup onto
        // the IO runtime rather than blocking `finish()`, since Python often
        // calls it from an asyncio context where blocking would stall the
        // event loop.
        let shuffle_dirs_rx = {
            let mut guard = self.shuffle_dirs.blocking_lock();
            guard.take()
        };
        if let Some(rx) = shuffle_dirs_rx {
            let runtime = common_runtime::get_io_runtime(true);
            // Detach the RuntimeTask handle — we don't need to await it; the
            // task will drive itself on the IO runtime.
            let _task = runtime.spawn(async move {
                if let Ok(dirs) = rx.await
                    && !dirs.is_empty()
                    && let Err(e) = crate::python::ray::clear_shuffle_dirs_on_all_nodes(dirs).await
                {
                    tracing::warn!("Failed to clear flight shuffle directories: {}", e);
                }
            });
        }
        let result = self.statistics_manager.export_metrics();
        Ok(PyExecutionStats::from(result))
    }
}

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlan", frozen)]
#[derive(Serialize, Deserialize)]
struct PyDistributedPhysicalPlan {
    plan: DistributedPhysicalPlan,
}

#[pymethods]
impl PyDistributedPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(
        builder: &PyLogicalPlanBuilder,
        query_id: String,
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let plan = DistributedPhysicalPlan::from_logical_plan_builder(
            &builder.builder,
            query_id.into(),
            config.config.clone(),
        )?;
        Ok(Self { plan })
    }

    fn idx(&self) -> String {
        self.plan.idx().to_string()
    }

    /// Output schema of the plan. Exposed so Python consumers can pass it to
    /// `PyFlightPartitionRef::fetch` when resolving flight refs produced by
    /// the iterator. Ray refs don't need schema; this is only read on the
    /// flight path.
    fn output_schema(&self) -> PyResult<daft_schema::python::schema::PySchema> {
        Ok(daft_schema::python::schema::PySchema {
            schema: self.plan.logical_plan().schema(),
        })
    }

    fn num_partitions(&self) -> PyResult<usize> {
        // Create pipeline nodes from the logical plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
            &Meter::global_scope("daft.execution.distributed.num_partitions"),
        )?;

        Ok(pipeline_node.num_partitions())
    }

    /// Visualize the distributed pipeline as ASCII text
    fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        // Create pipeline nodes from the logical plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
            &Meter::global_scope("daft.execution.distributed.repr_ascii"),
        )?;

        Ok(viz_distributed_pipeline_ascii(&pipeline_node, simple))
    }

    /// Visualize the distributed pipeline as Mermaid markdown
    fn repr_mermaid(&self, simple: bool, bottom_up: bool) -> PyResult<String> {
        // Create a pipeline node from the stage plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
            &Meter::global_scope("daft.execution.distributed.repr_mermaid"),
        )?;

        let display_level = if simple {
            DisplayLevel::Compact
        } else {
            DisplayLevel::Default
        };
        Ok(viz_distributed_pipeline_mermaid(
            &pipeline_node,
            display_level,
            bottom_up,
            None,
        ))
    }

    #[pyo3(signature = (psets=None))]
    fn repr_json(&self, psets: Option<HashMap<String, Vec<RayPartitionRef>>>) -> PyResult<String> {
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let psets = match psets {
            Some(psets) => Arc::new(
                psets
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            v.into_iter()
                                .map(|v| Arc::new(v) as Arc<dyn Partition>)
                                .collect(),
                        )
                    })
                    .collect(),
            ),
            None => Arc::new(HashMap::new()),
        };
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            psets,
            &Meter::global_scope("daft.execution.distributed.repr_json"),
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(serde_json::to_string(&pipeline_node.repr_json()).unwrap())
    }
}
impl_bincode_py_state_serialization!(PyDistributedPhysicalPlan);

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlanRunner", frozen)]
struct PyDistributedPhysicalPlanRunner {
    runner: Arc<PlanRunner<RaySwordfishWorker>>,
}

#[pymethods]
impl PyDistributedPhysicalPlanRunner {
    #[new]
    fn new() -> PyResult<Self> {
        let worker_manager = RayWorkerManager::new();
        Ok(Self {
            runner: Arc::new(PlanRunner::new(Arc::new(worker_manager))),
        })
    }

    fn run_plan(
        &self,
        py: Python,
        plan: &PyDistributedPhysicalPlan,
        psets: HashMap<String, Vec<RayPartitionRef>>,
    ) -> PyResult<PythonPartitionRefStream> {
        let psets = psets
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.into_iter()
                        .map(|v| Arc::new(v) as Arc<dyn Partition>)
                        .collect(),
                )
            })
            .collect();

        // Create subscribers list with progress bar always included
        let mut subscribers: Vec<Box<dyn StatisticsSubscriber>> =
            vec![Box::new(FlotillaProgressBar::try_new(py)?)];

        // Add the TaskLifecycleEventSubscriber if task emitting enabled
        if task_events_enabled() {
            subscribers.push(Box::new(TaskLifecycleEventSubscriber::new(
                plan.plan.query_id(),
            )));
        }

        // Only add DashboardStatisticsSubscriber if RAY_DISABLE_DASHBOARD is not set to "1"
        if std::env::var("RAY_DISABLE_DASHBOARD").as_deref() != Ok("1") {
            subscribers.push(Box::new(DashboardStatisticsSubscriber::new(
                plan.plan.query_id(),
            )));
        }

        let query_idx = plan.plan.idx();
        let query_id = plan.plan.query_id();
        let logical_plan = plan.plan.logical_plan().clone();

        let meter = Meter::query_scope(query_id, "daft.execution.distributed");

        let pipeline_node = logical_plan_to_pipeline_node(
            (&plan.plan).into(),
            logical_plan,
            Arc::new(psets),
            &meter,
        )?;

        let statistics_manager =
            StatisticsManager::from_pipeline_node(&pipeline_node, subscribers, &meter)?;

        let plan_result =
            self.runner
                .run_plan(query_idx, pipeline_node, statistics_manager.clone())?;

        let (stream, shuffle_dirs_rx) = plan_result.into_stream_and_cleanup();
        let part_stream = PythonPartitionRefStream {
            inner: Arc::new(Mutex::new(stream)),
            statistics_manager,
            shuffle_dirs: Arc::new(Mutex::new(Some(shuffle_dirs_rx))),
        };
        Ok(part_stream)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlan>()?;
    parent.add_class::<PyDistributedPhysicalPlanRunner>()?;
    parent.add_class::<RaySwordfishTask>()?;
    parent.add_class::<RaySwordfishWorker>()?;
    parent.add_class::<RayTaskResult>()?;
    Ok(())
}
