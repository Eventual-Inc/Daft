use std::sync::Arc;

use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use daft_micropartition::python::PyMicroPartition;
use pyo3::prelude::*;

use crate::{DaftContext, subscribers};

#[pyclass(frozen)(frozen)(frozen)]
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

    pub fn attach_subscriber(&self, py: Python, alias: String, subscriber: PyObject) {
        py.allow_threads(|| {
            self.inner.attach_subscriber(
                alias,
                Arc::new(subscribers::python::PySubscriberWrapper(subscriber)),
            );
        });
    }

    pub fn detach_subscriber(&self, py: Python, alias: &str) -> PyResult<()> {
        py.allow_threads(|| self.inner.detach_subscriber(alias))?;
        Ok(())
    }

    pub fn notify_query_start(
        &self,
        py: Python,
        query_id: String,
        unoptimized_plan: String,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.inner
                .notify_query_start(query_id.into(), unoptimized_plan.into())
        })?;
        Ok(())
    }

    pub fn notify_query_end(&self, py: Python, query_id: String) -> PyResult<()> {
        py.allow_threads(|| self.inner.notify_query_end(query_id.into()))?;
        Ok(())
    }

    pub fn notify_result_out(
        &self,
        py: Python,
        query_id: String,
        result: PyMicroPartition,
    ) -> PyResult<()> {
        py.allow_threads(|| self.inner.notify_result_out(query_id.into(), result.into()))?;
        Ok(())
    }

    pub fn notify_optimization_start(&self, py: Python, query_id: String) -> PyResult<()> {
        py.allow_threads(|| self.inner.notify_optimization_start(query_id.into()))?;
        Ok(())
    }

    pub fn notify_optimization_end(
        &self,
        py: Python,
        query_id: String,
        optimized_plan: String,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.inner
                .notify_optimization_end(query_id.into(), optimized_plan.into())
        })?;
        Ok(())
    }
}

impl From<DaftContext> for PyDaftContext {
    fn from(ctx: DaftContext) -> Self {
        Self { inner: ctx }
    }
}

impl From<PyDaftContext> for DaftContext {
    fn from(ctx: PyDaftContext) -> Self {
        ctx.inner
    }
}

impl<'a> From<&'a PyDaftContext> for &'a DaftContext {
    fn from(ctx: &'a PyDaftContext) -> Self {
        &ctx.inner
    }
}

#[pyfunction]
pub fn get_context() -> PyDaftContext {
    PyDaftContext {
        inner: super::get_context(),
    }
}
