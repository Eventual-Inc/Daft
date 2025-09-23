use std::{collections::HashMap, sync::Arc};

use pyo3::{Bound, IntoPyObject, PyAny, PyResult, Python, pyclass, pymethods};

use crate::{Stat, ops::NodeInfo};

#[pyclass]
pub enum StatType {
    Count,
    Bytes,
    Percent,
    Float,
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
