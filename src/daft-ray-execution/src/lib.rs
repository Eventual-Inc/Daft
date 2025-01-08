use std::{any::Any, sync::Arc};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionSet, PartitionSetCache};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_micropartition::python::PyMicroPartition;
use futures::stream::BoxStream;
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyIterator},
};

/// this is the python `MicroPartition` object, NOT the `PyMicroPartition` object, which is the native rust object
/// so yes, a rust wrapper around a python wrapper around a rust object
#[derive(Debug)]
pub struct WrappedPyMicroPartition {
    pub partition: PyObject,
}

impl Partition for WrappedPyMicroPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_bytes(&self) -> DaftResult<Option<usize>> {
        Python::with_gil(|py| {
            let native = self.partition.getattr(py, "_micropartition")?;
            let native = native.extract::<PyMicroPartition>(py)?;
            native.inner.size_bytes()
        })
    }
}

impl From<WrappedPyMicroPartition> for Arc<PyMicroPartition> {
    fn from(value: WrappedPyMicroPartition) -> Self {
        let partition = value.partition;
        Python::with_gil(|py| {
            let partition = partition.extract::<PyMicroPartition>(py).unwrap();
            Arc::new(partition)
        })
    }
}

#[derive(Debug)]
#[pyclass]
pub struct RayPartitionSetShim {
    ray_partition_set: PyObject,
}

impl RayPartitionSetShim {
    pub fn try_new() -> DaftResult<Self> {
        Python::with_gil(|py| {
            let ray_partition_set_module = py.import_bound("daft.runners.ray_runner")?;
            let ray_partition_set = ray_partition_set_module.getattr("RayPartitionSet")?;
            let instance = ray_partition_set.call0()?;
            let instance = instance.to_object(py);

            Ok(Self {
                ray_partition_set: instance,
            })
        })
    }
}

#[pymethods]
impl RayPartitionSetShim {
    #[new]
    fn __init__() -> PyResult<Self> {
        Self::try_new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))
    }

    pub fn run_iter(
        &self,
        py: Python,
        lp: PyObject,
        results_buffer_size: Option<usize>,
    ) -> PyResult<()> {
        let builder = lp.getattr(py, "_builder")?;
        let builder = builder.extract::<PyLogicalPlanBuilder>(py)?;
        let builder = builder.builder;

        Ok(())
    }
}

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

    fn run_iter_impl(
        &self,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<()> {
        let py_lp = PyLogicalPlanBuilder::new(lp);
        Python::with_gil(|py| {
            let builder = py.import_bound("daft.logical.builder")?;
            let builder = builder.getattr("LogicalPlanBuilder")?;
            let builder = builder.call((py_lp,), None)?;
            // let builder = builder.to_object(py);
            let result = self
                .ray_runner
                .call_method_bound(py, "run_iter", (builder, results_buffer_size), None)?
                .into_bound(py);

            let iter = PyIterator::from_bound_object(&result)?;
            for item in iter {
                println!("{:?}", item);
            }

            Ok(())
        })
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
        self.run_iter_impl(builder, None)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))?;

        Ok(py.None())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<RayRunnerShim>()?;
    parent.add_class::<RayPartitionSetShim>()?;
    Ok(())
}
