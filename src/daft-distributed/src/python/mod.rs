mod dashboard;
mod operator_lifecycle;
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
use daft_partition_refs::RayPartitionRef;
use dashboard::DashboardStatisticsSubscriber;
use futures::StreamExt;
use operator_lifecycle::{
    OperatorLifecycleEvent, OperatorLifecycleEventSubscriber, OperatorLifecycleQueue,
};
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
    lifecycle_queue: OperatorLifecycleQueue,
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
            let next = match next {
                Some(result) => {
                    let result = result?;
                    let ray_part_ref = result
                        .as_any()
                        .downcast_ref::<RayPartitionRef>()
                        .expect("Failed to downcast to RayPartitionRef")
                        .clone();
                    Some(ray_part_ref)
                }
                None => None,
            };
            Ok(next)
        })
    }

    /// Drain queued operator lifecycle transitions observed since the last
    /// drain. Returns a list of `(kind, node_id)` tuples where `kind` is
    /// `"start"` or `"end"`. The driver re-dispatches these on its own
    /// subscriber list so user-attached subscribers see events in real time
    /// instead of in a single batch at query end.
    fn take_lifecycle_events(&self) -> Vec<(&'static str, u32)> {
        self.lifecycle_queue
            .drain()
            .into_iter()
            .map(|e| match e {
                OperatorLifecycleEvent::Start { node_id } => ("start", node_id),
                OperatorLifecycleEvent::End { node_id } => ("end", node_id),
            })
            .collect()
    }

    /// Emit `End` for every node that started but hasn't ended yet, then
    /// drain. Called once the partition stream is exhausted so a final batch
    /// of End events is delivered to the driver before query end.
    fn finalize_lifecycle_events(&self) -> Vec<(&'static str, u32)> {
        self.lifecycle_queue.finalize_remaining();
        self.take_lifecycle_events()
    }

    fn finish(&self) -> PyResult<PyExecutionStats> {
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

        // Lifecycle queue is shared between the per-task subscriber and the
        // partition stream the driver drains over Ray.
        let lifecycle_queue = OperatorLifecycleQueue::new();

        // Create subscribers list with progress bar always included
        let mut subscribers: Vec<Box<dyn StatisticsSubscriber>> = vec![
            Box::new(FlotillaProgressBar::try_new(py)?),
            Box::new(OperatorLifecycleEventSubscriber::new(
                lifecycle_queue.clone(),
            )),
        ];

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

        let part_stream = PythonPartitionRefStream {
            inner: Arc::new(Mutex::new(plan_result.into_stream())),
            statistics_manager,
            lifecycle_queue,
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
