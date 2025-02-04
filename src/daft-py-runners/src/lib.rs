//! Wrapper around the python RayRunner class
use std::sync::Arc;

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
#[derive(Debug)]
pub struct RayRunner {
    pub ray_runner: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl RayRunner {
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
                ray_runner: Arc::new(instance),
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

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct NativeRunner {
    pub instance: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl NativeRunner {
    pub fn try_new() -> DaftResult<Self> {
        Python::with_gil(|py| {
            let native_runner_module = py.import(intern!(py, "daft.runners.native_runner"))?;
            let native_runner = native_runner_module.getattr(intern!(py, "NativeRunner"))?;

            let instance = native_runner.call0()?;
            let instance = instance.unbind();

            Ok(Self {
                instance: Arc::new(instance),
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
        let result = self.instance.call_method(
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

#[derive(Debug)]
#[cfg(feature = "python")]
pub enum Runner {
    Ray(RayRunner),
    Native(NativeRunner),
}

#[cfg(feature = "python")]
impl Runner {
    pub fn run_iter_impl(
        &self,
        py: Python<'_>,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<Vec<DaftResult<MicroPartitionRef>>> {
        match self {
            Runner::Ray(ray) => ray.run_iter_impl(py, lp, results_buffer_size),
            Runner::Native(native) => native.run_iter_impl(py, lp, results_buffer_size),
        }
    }
}

#[derive(Debug)]
#[cfg(feature = "python")]
pub enum RunnerConfig {
    Native,
    Ray {
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    },
}

impl RunnerConfig {
    pub fn create_runner(self) -> DaftResult<Runner> {
        match self {
            RunnerConfig::Native => Ok(Runner::Native(NativeRunner::try_new()?)),
            RunnerConfig::Ray {
                address,
                max_task_backlog,
                force_client_mode,
            } => Ok(Runner::Ray(RayRunner::try_new(
                address,
                max_task_backlog,
                force_client_mode,
            )?)),
        }
    }
}
