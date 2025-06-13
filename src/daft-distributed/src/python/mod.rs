mod ray;

use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::Partition;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use futures::StreamExt;
use pyo3::prelude::*;
use ray::{RayPartitionRef, RaySwordfishTask, RaySwordfishWorker, RayWorkerManager};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::plan::{DistributedPhysicalPlan, PlanResultStream, PlanRunner};

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
            Python::with_gil(|py| {
                let next = match next {
                    Some(result) => {
                        let result = result?;
                        let ray_part_ref = result
                            .partition()
                            .as_any()
                            .downcast_ref::<RayPartitionRef>()
                            .expect("Failed to downcast to RayPartitionRef");
                        let objref = ray_part_ref.object_ref.clone_ref(py);
                        let size_bytes = ray_part_ref.size_bytes;
                        let num_rows = ray_part_ref.num_rows;
                        let ret = (objref, num_rows, size_bytes);
                        Some(ret)
                    }
                    None => None,
                };
                Ok(next)
            })
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
}

#[pymethods]
impl PyDistributedPhysicalPlanRunner {
    #[new]
    fn new() -> PyResult<Self> {
        let worker_manager = RayWorkerManager::try_new()?;
        Ok(Self {
            runner: Arc::new(PlanRunner::new(Arc::new(worker_manager))),
        })
    }

    fn run_plan(
        &self,
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
        let plan_result = self.runner.run_plan(&plan.plan, psets)?;
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
    Ok(())
}
