use std::sync::Arc;

use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use common_error::DaftError;
use pyo3::prelude::*;

use crate::{DaftContext, Runner, RunnerConfig};

#[pyclass]
pub struct PyRunnerConfig {
    _inner: RunnerConfig,
}

#[pyclass]
pub struct PyDaftContext {
    inner: crate::DaftContext,
}

impl Default for PyDaftContext {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl PyDaftContext {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: crate::get_context(),
        }
    }

    pub fn get_or_create_runner(&self, py: Python) -> PyResult<PyObject> {
        let runner = py.allow_threads(|| self.inner.get_or_create_runner())?;

        match runner.as_ref() {
            Runner::Ray(ray) => {
                let pyobj = ray.pyobj.as_ref();
                Ok(pyobj.clone_ref(py))
            }
            Runner::Native(native) => {
                let pyobj = native.pyobj.as_ref();
                Ok(pyobj.clone_ref(py))
            }
        }
    }
    #[getter(_daft_execution_config)]
    pub fn get_daft_execution_config(&self, py: Python) -> PyResult<PyDaftExecutionConfig> {
        let config = py.allow_threads(|| self.inner.execution_config());
        let config = PyDaftExecutionConfig { config };
        Ok(config)
    }

    #[getter(_daft_planning_config)]
    pub fn get_daft_planning_config(&self, py: Python) -> PyResult<PyDaftPlanningConfig> {
        let config = py.allow_threads(|| self.inner.planning_config());
        let config = PyDaftPlanningConfig { config };
        Ok(config)
    }

    #[setter(_daft_execution_config)]
    pub fn set_daft_execution_config(&self, py: Python, config: PyDaftExecutionConfig) {
        py.allow_threads(|| self.inner.set_execution_config(config.config));
    }

    #[setter(_daft_planning_config)]
    pub fn set_daft_planning_config(&self, py: Python, config: PyDaftPlanningConfig) {
        py.allow_threads(|| self.inner.set_planning_config(config.config));
    }

    #[getter(_runner)]
    pub fn get_runner(&self, py: Python) -> Option<PyObject> {
        let runner = py.allow_threads(|| self.inner.runner());
        runner.map(|r| r.to_pyobj(py))
    }

    #[setter(_runner)]
    pub fn set_runner(&self, py: Python, runner: PyObject) -> PyResult<()> {
        let runner = Runner::from_pyobj(runner)?;
        let runner = Arc::new(runner);
        py.allow_threads(|| self.inner.set_runner(runner))?;
        Ok(())
    }
}
impl From<DaftContext> for PyDaftContext {
    fn from(ctx: DaftContext) -> Self {
        Self { inner: ctx }
    }
}

#[pyfunction]
pub fn get_runner_config_from_env() -> PyResult<PyRunnerConfig> {
    Ok(PyRunnerConfig {
        _inner: super::get_runner_config_from_env()?,
    })
}

#[pyfunction]
pub fn get_context() -> PyDaftContext {
    PyDaftContext {
        inner: super::get_context(),
    }
}

#[pyfunction(signature = (
    address = None,
    noop_if_initialized = false,
    max_task_backlog = None,
    force_client_mode = false
))]
pub fn set_runner_ray(
    address: Option<String>,
    noop_if_initialized: Option<bool>,
    max_task_backlog: Option<usize>,
    force_client_mode: Option<bool>,
) -> PyResult<PyDaftContext> {
    let noop_if_initialized = noop_if_initialized.unwrap_or(false);
    let context = super::set_runner_ray(address, max_task_backlog, force_client_mode);
    match context {
        Ok(ctx) => Ok(ctx.into()),
        Err(e)
            if noop_if_initialized
                && matches!(&e, DaftError::InternalError(msg) if msg.contains("Cannot set runner more than once")) =>
        {
            Ok(super::get_context().into())
        }
        Err(e) => Err(e.into()),
    }
}

#[pyfunction(signature = (num_threads = None))]
pub fn set_runner_native(num_threads: Option<usize>) -> PyResult<PyDaftContext> {
    let ctx = super::set_runner_native(num_threads)?;
    Ok(ctx.into())
}
