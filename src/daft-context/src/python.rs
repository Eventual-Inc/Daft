use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use common_error::DaftError;
use daft_runners::{NativeRunner, RayRunner};
use pyo3::{IntoPyObjectExt, prelude::*};

use crate::{DaftContext, Runner, RunnerConfig, detect_ray_state};

#[pyclass]
pub struct PyRunnerConfig {
    _inner: RunnerConfig,
}

#[pyclass]
pub struct PyDaftContext {
    inner: DaftContext,
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

    pub fn get_or_infer_runner_type(&self, py: Python) -> PyResult<PyObject> {
        match self.inner.runner() {
            Some(runner) => match runner.as_ref() {
                Runner::Ray(_) => RayRunner::NAME,
                Runner::Native(_) => NativeRunner::NAME,
            },
            None => {
                if let (true, _) = detect_ray_state() {
                    RayRunner::NAME
                } else {
                    todo!()
                }
            }
        }
        .into_py_any(py)
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
}
impl From<DaftContext> for PyDaftContext {
    fn from(ctx: DaftContext) -> Self {
        Self { inner: ctx }
    }
}

#[pyfunction]
pub fn get_context() -> PyDaftContext {
    PyDaftContext {
        inner: super::get_context(),
    }
}
