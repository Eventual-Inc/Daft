use std::{collections::HashMap, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::{Bound, IntoPyObject, PyAny, PyResult, Python, pyclass, pymethods};
use serde::{Deserialize, Serialize};

use crate::{Stat, operator_metrics::OperatorMetrics, ops::NodeInfo};

#[pyclass(eq, eq_int)]
#[derive(PartialEq, Eq)]
pub enum StatType {
    #[pyo3(name = "UPPERCASE")]
    Count = 0,
    #[pyo3(name = "UPPERCASE")]
    Bytes,
    #[pyo3(name = "UPPERCASE")]
    Percent,
    #[pyo3(name = "UPPERCASE")]
    Float,
    #[pyo3(name = "UPPERCASE")]
    Duration,
}

impl Stat {
    pub fn into_py_contents(self, py: Python<'_>) -> PyResult<(StatType, Bound<'_, PyAny>)> {
        match self {
            Self::Count(value) => Ok((StatType::Count, value.into_pyobject(py)?.into_any())),
            Self::Bytes(value) => Ok((StatType::Bytes, value.into_pyobject(py)?.into_any())),
            Self::Percent(value) => Ok((StatType::Percent, value.into_pyobject(py)?.into_any())),
            Self::Float(value) => Ok((StatType::Float, value.into_pyobject(py)?.into_any())),
            Self::Duration(value) => Ok((StatType::Duration, value.into_pyobject(py)?.into_any())),
        }
    }
}

#[pyclass(frozen)]
pub struct PyNodeInfo {
    node_info: Arc<NodeInfo>,
}

#[pymethods]
impl PyNodeInfo {
    #[getter]
    pub fn id(&self) -> usize {
        self.node_info.id
    }
    #[getter]
    pub fn name(&self) -> String {
        self.node_info.name.to_string()
    }
    #[getter]
    pub fn node_type(&self) -> String {
        self.node_info.node_type.to_string()
    }
    #[getter]
    pub fn node_category(&self) -> String {
        self.node_info.node_category.to_string()
    }
    #[getter]
    pub fn context(&self) -> HashMap<String, String> {
        self.node_info.context.clone()
    }
}

impl From<Arc<NodeInfo>> for PyNodeInfo {
    fn from(node_info: Arc<NodeInfo>) -> Self {
        Self { node_info }
    }
}

#[pyclass(module = "daft.daft", name = "OperatorMetrics")]
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PyOperatorMetrics {
    pub inner: OperatorMetrics,
}

#[pymethods]
impl PyOperatorMetrics {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: OperatorMetrics::default(),
        }
    }

    #[pyo3(signature = (name, value, *, description=None, attributes=None))]
    pub fn inc_counter(
        &mut self,
        name: &str,
        value: u64,
        description: Option<&str>,
        attributes: Option<HashMap<String, String>>,
    ) {
        self.inner.inc_counter(name, value, description, attributes);
    }
}

impl_bincode_py_state_serialization!(PyOperatorMetrics);
