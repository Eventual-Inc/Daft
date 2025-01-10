//! Wrapper around the python RayRunner class
#[cfg(feature = "python")]
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
#[cfg(feature = "python")]
use daft_micropartition::{python::PyMicroPartition, MicroPartitionRef};
#[cfg(feature = "python")]
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyIterator},
};

#[cfg(feature = "python")]
pub struct RayEngine {
    ray_runner: PyObject,
}

#[cfg(feature = "python")]
impl RayEngine {
    pub fn try_new(
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let ray_runner_module = py.import(intern!(py, "daft.runners.ray_runner"))?;
            let ray_runner = ray_runner_module.getattr(intern!(py, "RayRunner"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "address"), address)?;
            kwargs.set_item(intern!(py, "max_task_backlog"), max_task_backlog)?;
            kwargs.set_item(intern!(py, "force_client_mode"), force_client_mode)?;

            let instance = ray_runner.call((), Some(&kwargs))?;
            let instance = instance.unbind();

            Ok(Self {
                ray_runner: instance,
            })
        })
    }

    pub fn run_iter_impl(
        &self,
        py: Python<'_>,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<Vec<DaftResult<MicroPartitionRef>>> {
        let py_lp = PyLogicalPlanBuilder::new(lp);
        let builder = py.import(intern!(py, "daft.logical.builder"))?;
        let builder = builder.getattr(intern!(py, "LogicalPlanBuilder"))?;
        let builder = builder.call((py_lp,), None)?;
        let result = self.ray_runner.call_method(
            py,
            intern!(py, "run_iter_tables"),
            (builder, results_buffer_size),
            None,
        )?;

        let result = result.bind(py);
        let iter = PyIterator::from_object(result)?;

        let iter = iter.map(|item| {
            let item = item?;
            let partition = item.getattr(intern!(py, "_micropartition"))?;
            let partition = partition.extract::<PyMicroPartition>()?;
            let partition = partition.inner;
            Ok::<_, DaftError>(partition)
        });

        Ok(iter.collect())
    }
}
