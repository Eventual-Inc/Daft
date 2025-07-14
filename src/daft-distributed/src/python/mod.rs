mod progress_bar;
mod ray;
use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::Partition;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use futures::StreamExt;
use progress_bar::FlotillaProgressBar;
use pyo3::prelude::*;
use ray::{RayPartitionRef, RaySwordfishTask, RaySwordfishWorker, RayWorkerManager};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    plan::{DistributedPhysicalPlan, PlanResultStream, PlanRunner},
    python::ray::RayTaskResult,
    statistics::{HttpSubscriber, StatisticsManager, StatisticsSubscriber},
};

#[pyclass(frozen)]
struct PythonPartitionRefStream {
    inner: Arc<Mutex<PlanResultStream>>,
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
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let plan = DistributedPhysicalPlan::from_logical_plan_builder(
            &builder.builder,
            config.config.clone(),
        )?;
        Ok(Self { plan })
    }

    fn id(&self) -> String {
        self.plan.id().to_string()
    }

    /// Visualize the distributed pipeline as ASCII text
    fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        // Create a pipeline node from the stage plan
        let stage_plan = self.plan.stage_plan();
        Ok(stage_plan.repr_ascii(self.plan.id().into(), simple)?)
    }

    /// Visualize the distributed pipeline as Mermaid markdown
    fn repr_mermaid(&self, simple: bool, bottom_up: bool) -> PyResult<String> {
        // Create a pipeline node from the stage plan
        let stage_plan = self.plan.stage_plan();
        Ok(stage_plan.repr_mermaid(self.plan.id().into(), simple, bottom_up)?)
    }
}
impl_bincode_py_state_serialization!(PyDistributedPhysicalPlan);

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlanRunner", frozen)]
struct PyDistributedPhysicalPlanRunner {
    runner: Arc<PlanRunner<RaySwordfishWorker>>,
    on_ray_actor: bool,
}

#[pymethods]
impl PyDistributedPhysicalPlanRunner {
    #[new]
    fn new(py: Python, on_ray_actor: bool) -> PyResult<Self> {
        let worker_manager = RayWorkerManager::try_new(py)?;
        Ok(Self {
            runner: Arc::new(PlanRunner::new(Arc::new(worker_manager))),
            on_ray_actor,
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

        let mut subscribers: Vec<Box<dyn StatisticsSubscriber>> = vec![Box::new(
            FlotillaProgressBar::try_new(py, self.on_ray_actor)?,
        )];

        tracing::info!("Checking DAFT_DASHBOARD_URL environment variable");
        match std::env::var("DAFT_DASHBOARD_URL") {
            Ok(url) => {
                tracing::info!("DAFT_DASHBOARD_URL is set to: {}", url);
                tracing::info!("Adding HttpSubscriber to statistics manager");
                subscribers.push(Box::new(HttpSubscriber::new()));
            }
            Err(_) => {
                tracing::info!("DAFT_DASHBOARD_URL not set, skipping HttpSubscriber");
            }
        }

        let statistics_manager = StatisticsManager::new(subscribers);
        let plan_result = self
            .runner
            .run_plan(&plan.plan, psets, statistics_manager)?;
        let part_stream = PythonPartitionRefStream {
            inner: Arc::new(Mutex::new(plan_result.into_stream())),
        };
        Ok(part_stream)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlan>()?;
    parent.add_class::<PyDistributedPhysicalPlanRunner>()?;
    parent.add_class::<RaySwordfishTask>()?;
    parent.add_class::<RayPartitionRef>()?;
    parent.add_class::<RaySwordfishWorker>()?;
    parent.add_class::<RayTaskResult>()?;
    Ok(())
}
