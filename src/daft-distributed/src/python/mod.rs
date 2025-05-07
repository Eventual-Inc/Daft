mod ray;

use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::Partition;
use daft_logical_plan::PyLogicalPlanBuilder;
use futures::StreamExt;
use pyo3::prelude::*;
use ray::{RayPartitionRef, RaySwordfishTask, RayWorkerManagerFactory};
use tokio::sync::Mutex;

use crate::plan::{DistributedPhysicalPlan, PlanResult};

#[pyclass(frozen)]
struct PythonPartitionRefStream {
    inner: Arc<Mutex<PlanResult>>,
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
                            .as_any()
                            .downcast_ref::<RayPartitionRef>()
                            .expect("Failed to downcast to RayPartitionRef");
                        let objref = ray_part_ref.object_ref.clone_ref(py);
                        let size_bytes = ray_part_ref.size_bytes;
                        let num_rows = ray_part_ref.num_rows;
                        let ret = (objref, size_bytes, num_rows);
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
struct PyDistributedPhysicalPlan {
    planner: DistributedPhysicalPlan,
}

#[pymethods]
impl PyDistributedPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(
        builder: &PyLogicalPlanBuilder,
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let planner = DistributedPhysicalPlan::from_logical_plan_builder(
            &builder.builder,
            config.config.clone(),
        )?;
        Ok(Self { planner })
    }

    fn run_plan(
        &self,
        psets: HashMap<String, Vec<RayPartitionRef>>,
        py: Python,
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
        let daft_execution_config = self.planner.execution_config().clone();
        let worker_manager_factory = RayWorkerManagerFactory::new(
            daft_execution_config,
            pyo3_async_runtimes::tokio::get_current_locals(py)
                .expect("Failed to get current task locals"),
        );
        let part_stream = self
            .planner
            .run_plan(psets, Box::new(worker_manager_factory))?;
        let part_stream = PythonPartitionRefStream {
            inner: Arc::new(Mutex::new(part_stream)),
        };
        Ok(part_stream)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlan>()?;
    parent.add_class::<RaySwordfishTask>()?;
    parent.add_class::<RayPartitionRef>()?;
    Ok(())
}
