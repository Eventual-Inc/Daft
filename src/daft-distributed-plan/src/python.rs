use std::sync::Arc;

use common_daft_config::PyDaftExecutionConfig;
use common_error::DaftResult;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_local_plan::PyLocalPhysicalPlan;
use daft_logical_plan::PyLogicalPlanBuilder;
use itertools::Itertools;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::ray::ray_task_handle::RayPartitionRef;
#[cfg(feature = "python")]
use crate::ray::ray_worker_manager::RayWorkerManager;
use crate::{plan::DistributedPhysicalPlan, task::Task};

#[cfg(feature = "python")]
#[pyclass]
struct PythonIterator {
    iter: Box<dyn Iterator<Item = DaftResult<PyObject>> + Send + Sync>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PythonIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        let iter = &mut slf.iter;
        Ok(py.allow_threads(|| iter.next().transpose())?)
    }
}

#[pyclass(module = "daft.daft", name = "SwordfishWorkerTask")]
#[derive(Serialize, Deserialize)]
pub(crate) struct PySwordfishWorkerTask {
    pub task: Task,
}

impl_bincode_py_state_serialization!(PySwordfishWorkerTask);

#[pymethods]
impl PySwordfishWorkerTask {
    fn estimated_memory_cost(&self) -> usize {
        self.task.estimated_memory_cost()
    }

    fn plan(&self) -> PyResult<PyLocalPhysicalPlan> {
        let plan = self.task.plan();
        Ok(PyLocalPhysicalPlan { plan })
    }

    fn execution_config(&self) -> PyResult<PyDaftExecutionConfig> {
        let config = self.task.execution_config();
        Ok(PyDaftExecutionConfig { config })
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

    pub fn run_plan(&mut self) -> PyResult<PythonIterator> {
        let ray_worker_manager = RayWorkerManager::new();
        let results = self
            .planner
            .run_plan(Arc::new(ray_worker_manager))?
            .flatten_ok();
        let iter = Box::new(results.map(|result| {
            let result = result?;
            let ray_part_ref = result.as_any().downcast_ref::<RayPartitionRef>().unwrap();
            pyo3::Python::with_gil(|py| {
                let objref = ray_part_ref.object_ref.clone_ref(py);
                let size_bytes = ray_part_ref._size_bytes;
                let num_rows = ray_part_ref._num_rows;
                Ok((objref, size_bytes, num_rows)
                    .into_pyobject(py)?
                    .unbind()
                    .into_any())
            })
        }));
        let part_iter = PythonIterator { iter };
        Ok(part_iter)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlan>()?;
    parent.add_class::<PySwordfishWorkerTask>()?;
    Ok(())
}
