use std::sync::Arc;

use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
use common_partitioning::python::PyPartitionRef;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_core::python::PySchema;
use pyo3::{Py, PyAny, prelude::*};
use serde::{Deserialize, Serialize};
use subscribers::python::PySubscriberWrapper;

use crate::{ContextState, DaftContext, subscribers, subscribers::QueryMetadata};

#[pyclass(module = "daft.daft", frozen)]
#[derive(Clone, Serialize, Deserialize)]
pub struct PyQueryMetadata(pub(crate) Arc<QueryMetadata>);
impl_bincode_py_state_serialization!(PyQueryMetadata);

#[pymethods]
impl PyQueryMetadata {
    #[new]
    #[pyo3(signature = (output_schema, unoptimized_plan))]
    fn __new__(output_schema: PySchema, unoptimized_plan: &str) -> Self {
        Self(Arc::new(QueryMetadata {
            output_schema: output_schema.into(),
            unoptimized_plan: unoptimized_plan.into(),
        }))
    }
    #[getter]
    pub fn output_schema(&self) -> PySchema {
        self.0.output_schema.clone().into()
    }
    #[getter]
    pub fn unoptimized_plan(&self) -> String {
        self.0.unoptimized_plan.to_string()
    }
}

impl From<Arc<QueryMetadata>> for PyQueryMetadata {
    fn from(metadata: Arc<QueryMetadata>) -> Self {
        Self(metadata)
    }
}

impl From<PyQueryMetadata> for Arc<QueryMetadata> {
    fn from(metadata: PyQueryMetadata) -> Self {
        metadata.0
    }
}

impl<'a> From<&'a PyQueryMetadata> for &'a Arc<QueryMetadata> {
    fn from(ctx: &'a PyQueryMetadata) -> Self {
        &ctx.0
    }
}

#[pyclass(module = "daft.daft", frozen)]
#[derive(Serialize, Deserialize)]
pub struct PyContextState(ContextState);

impl_bincode_py_state_serialization!(PyContextState);

impl From<ContextState> for PyContextState {
    fn from(state: ContextState) -> Self {
        Self(state)
    }
}

impl From<PyContextState> for ContextState {
    fn from(state: PyContextState) -> Self {
        state.0
    }
}

impl<'a> From<&'a PyContextState> for &'a ContextState {
    fn from(state: &'a PyContextState) -> Self {
        &state.0
    }
}

#[pyclass(module = "daft.daft", frozen)]
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
        let config = py.detach(|| self.inner.execution_config());
        let config = PyDaftExecutionConfig { config };
        Ok(config)
    }

    #[getter(_daft_planning_config)]
    pub fn get_daft_planning_config(&self, py: Python) -> PyResult<PyDaftPlanningConfig> {
        let config = py.detach(|| self.inner.planning_config());
        let config = PyDaftPlanningConfig { config };
        Ok(config)
    }

    #[setter(_daft_execution_config)]
    pub fn set_daft_execution_config(&self, py: Python, config: PyDaftExecutionConfig) {
        py.detach(|| self.inner.set_execution_config(config.config));
    }

    #[setter(_daft_planning_config)]
    pub fn set_daft_planning_config(&self, py: Python, config: PyDaftPlanningConfig) {
        py.detach(|| self.inner.set_planning_config(config.config));
    }

    #[getter]
    pub fn state(&self, py: Python) -> PyResult<PyContextState> {
        let state = py.detach(|| self.inner.state());
        Ok(PyContextState(state))
    }

    pub fn attach_subscriber(&self, py: Python, alias: String, subscriber: Py<PyAny>) {
        py.detach(|| {
            self.inner
                .attach_subscriber(alias, PySubscriberWrapper::new(subscriber));
        });
    }

    pub fn detach_subscriber(&self, py: Python, alias: &str) -> PyResult<()> {
        py.detach(|| self.inner.detach_subscriber(alias))?;
        Ok(())
    }

    pub fn notify_query_start(
        &self,
        py: Python,
        query_id: String,
        metadata: PyQueryMetadata,
    ) -> PyResult<()> {
        py.detach(|| {
            self.inner
                .notify_query_start(query_id.into(), metadata.into())
        })?;
        Ok(())
    }

    pub fn notify_query_end(&self, py: Python, query_id: String) -> PyResult<()> {
        py.detach(|| self.inner.notify_query_end(query_id.into()))?;
        Ok(())
    }

    pub fn notify_result_out(
        &self,
        py: Python,
        query_id: String,
        result: PyPartitionRef,
    ) -> PyResult<()> {
        py.detach(|| self.inner.notify_result_out(query_id.into(), result.into()))?;
        Ok(())
    }

    pub fn notify_optimization_start(&self, py: Python, query_id: String) -> PyResult<()> {
        py.detach(|| self.inner.notify_optimization_start(query_id.into()))?;
        Ok(())
    }

    pub fn notify_optimization_end(
        &self,
        py: Python,
        query_id: String,
        optimized_plan: String,
    ) -> PyResult<()> {
        py.detach(|| {
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
