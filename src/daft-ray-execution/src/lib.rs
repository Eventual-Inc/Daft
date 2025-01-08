use std::{any::Any, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_partitioning::{Partition, PartitionSet, PartitionSetCache};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_micropartition::{python::PyMicroPartition, MicroPartition, MicroPartitionRef};
use futures::stream::BoxStream;
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyIterator},
};

#[pyclass]
pub struct RayRunnerShim {
    ray_runner: PyObject,
}

impl RayRunnerShim {
    pub fn try_new(
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let ray_runner_module = py.import_bound("daft.runners.ray_runner")?;
            let ray_runner = ray_runner_module.getattr("RayRunner")?;
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("address", address)?;
            kwargs.set_item("max_task_backlog", max_task_backlog)?;
            kwargs.set_item("force_client_mode", force_client_mode)?;

            let instance = ray_runner.call((), Some(&kwargs))?;
            let instance = instance.to_object(py);

            Ok(Self {
                ray_runner: instance,
            })
        })
    }

    pub fn run_iter_impl<'a>(
        &self,
        py: Python<'a>,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<Vec<DaftResult<MicroPartitionRef>>> {
        let py_lp = PyLogicalPlanBuilder::new(lp);
        let builder = py.import_bound("daft.logical.builder")?;
        let builder = builder.getattr("LogicalPlanBuilder")?;
        let builder = builder.call((py_lp,), None)?;

        let result = self
            .ray_runner
            .call_method_bound(py, "run_iter_tables", (builder, results_buffer_size), None)?
            .into_bound(py);

        let iter = PyIterator::from_bound_object(&result)?;

        let iter = iter.into_iter().map(|item| {
            let item = item?;
            let partition = item.getattr("_micropartition")?;
            let partition = partition.extract::<PyMicroPartition>()?;
            let partition = partition.inner;
            Ok::<_, DaftError>(partition)
        });

        Ok(iter.collect())
    }
}

#[pymethods]
impl RayRunnerShim {
    #[new]
    fn __init__(
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> PyResult<Self> {
        Self::try_new(address, max_task_backlog, force_client_mode)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))
    }

    pub fn run_iter(&self, py: Python, builder: PyObject) -> PyResult<PyObject> {
        let builder = builder.getattr(py, "_builder")?;
        let builder = builder.extract::<PyLogicalPlanBuilder>(py)?;
        let builder = builder.builder;
        let iter = self
            .run_iter_impl(py, builder, None)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))?;

        Ok(py.None())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<RayRunnerShim>()?;
    Ok(())
}
