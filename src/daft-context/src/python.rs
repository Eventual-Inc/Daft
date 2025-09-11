use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use pyo3::prelude::*;

use crate::DaftContext;

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
