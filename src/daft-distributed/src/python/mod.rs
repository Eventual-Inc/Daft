mod ray;

use std::{collections::HashMap, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_partitioning::Partition;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;
use ray::{RayPartitionRef, RaySwordfishTask, RayWorkerManager};
use tokio::sync::Mutex;

use crate::{
    plan::distributed_plan::{DistributedPhysicalPlan, PlanResultProducer},
    runtime::get_or_init_task_locals,
    scheduling::worker::WorkerManager,
};

#[pyclass]
struct PythonPartitionRefStream {
    inner: Arc<Mutex<PlanResultProducer>>,
}

#[pymethods]
impl PythonPartitionRefStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = {
                let inner = inner.lock().await;
                inner.get_next().await
            };
            Python::with_gil(|py| {
                let next = match next {
                    Some(result) => {
                        let result = result?;
                        let ray_part_ref =
                            result.as_any().downcast_ref::<RayPartitionRef>().unwrap();
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

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlan")]
struct PyDistributedPhysicalPlan {
    planner: DistributedPhysicalPlan,
}

#[pymethods]
impl PyDistributedPhysicalPlan {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        builder: &PyLogicalPlanBuilder,
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let planner =
            DistributedPhysicalPlan::from_logical_plan_builder(&builder.builder, &config.config)?;
        Ok(Self { planner })
    }

    pub fn run_plan(
        &mut self,
        psets: HashMap<String, Vec<RayPartitionRef>>,
        py: Python,
    ) -> PyResult<PythonPartitionRefStream> {
        let _ = get_or_init_task_locals(py);
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
        let worker_manager_creator =
            Arc::new(|| Box::new(RayWorkerManager::new()) as Box<dyn WorkerManager>);
        let part_stream = self.planner.run_plan(psets, worker_manager_creator);
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
