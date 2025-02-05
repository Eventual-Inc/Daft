use std::sync::Arc;

use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

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
        let mut lock = self
            .inner
            .state
            .write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))?;

        let runner = lock.get_or_create_runner()?;
        match runner.as_ref() {
            Runner::Ray(ray) => {
                let pyobj = ray.pyobj.as_ref();
                Ok(pyobj.clone_ref(py))
            }
            Runner::Native(native) => {
                let pyobj = native.pyobj.as_ref();
                Ok(pyobj.clone_ref(py))
            }
            Runner::Py(py_runner) => {
                let pyobj = py_runner.pyobj.as_ref();
                Ok(pyobj.clone_ref(py))
            }
        }
    }
    #[getter(_daft_execution_config)]
    pub fn get_daft_execution_config(&self) -> PyResult<PyDaftExecutionConfig> {
        let state = self.inner.state.read().unwrap();
        let config = state.config.execution.clone();
        let config = PyDaftExecutionConfig { config };
        Ok(config)
    }

    #[getter(_daft_planning_config)]
    pub fn get_daft_planning_config(&self) -> PyResult<PyDaftPlanningConfig> {
        let state = self.inner.state.read().unwrap();
        let config = state.config.planning.clone();
        let config = PyDaftPlanningConfig { config };
        Ok(config)
    }

    #[setter(_daft_execution_config)]
    pub fn set_daft_execution_config(&self, config: PyDaftExecutionConfig) {
        let mut state = self.inner.state.write().unwrap();
        state.config.execution = config.config;
    }

    #[setter(_daft_planning_config)]
    pub fn set_daft_planning_config(&self, config: PyDaftPlanningConfig) {
        let mut state = self.inner.state.write().unwrap();
        state.config.planning = config.config;
    }

    #[getter(_runner)]
    pub fn get_runner(&self, py: Python) -> Option<PyObject> {
        let state = self.inner.state.read().unwrap();
        state.runner.clone().map(|r| r.to_pyobj(py))
    }

    #[setter(_runner)]
    pub fn set_runner(&self, runner: Option<PyObject>) -> PyResult<()> {
        if let Some(runner) = runner {
            let runner = Runner::from_pyobj(runner)?;
            let runner = Arc::new(runner);
            self.inner.set_runner(runner)?;
            Ok(())
        } else {
            self.inner.state.write().unwrap().runner = None;
            Ok(())
        }
    }
}
impl From<DaftContext> for PyDaftContext {
    fn from(ctx: DaftContext) -> Self {
        Self { inner: ctx }
    }
}

#[pyfunction]
pub fn get_runner_config_from_env() -> PyRunnerConfig {
    PyRunnerConfig {
        _inner: super::get_runner_config_from_env(),
    }
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
    let res =
        super::set_runner_ray(address, max_task_backlog, force_client_mode).map(|ctx| ctx.into());
    if noop_if_initialized {
        match res {
            Err(_) => Ok(super::get_context().into()),
            Ok(ctx) => Ok(ctx),
        }
    } else {
        res.map_err(|_| PyRuntimeError::new_err("Cannot set runner more than once"))
    }
}

#[pyfunction]
pub fn set_runner_native() -> PyResult<PyDaftContext> {
    super::set_runner_native()
        .map(|ctx| ctx.into())
        .map_err(|_| PyRuntimeError::new_err("Cannot set runner more than once"))
}

#[pyfunction(signature = (use_thread_pool = None))]
pub fn set_runner_py(use_thread_pool: Option<bool>) -> PyResult<PyDaftContext> {
    super::set_runner_py(use_thread_pool)
        .map(|ctx| ctx.into())
        .map_err(|_| PyRuntimeError::new_err("Cannot set runner more than once"))
}
