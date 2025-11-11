use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use pyo3::{
    Bound, IntoPyObject, Py, PyAny, PyResult, Python, pyclass, pymethods,
    types::{PyAnyMethods, PyDict},
};

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

#[pyclass]
#[derive(Clone, Default)]
pub struct PyMetricsCollector {
    pub(crate) inner: Arc<Mutex<OperatorMetrics>>,
}

#[pymethods]
impl PyMetricsCollector {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OperatorMetrics::default())),
        }
    }

    pub fn payload(&self, py: Python) -> PyResult<Py<PyDict>> {
        let metrics = self
            .inner
            .lock()
            .expect("Failed to lock metrics sink")
            .clone();
        let payload = PyDict::new(py);
        let counters = metrics.into_inner();
        if !counters.is_empty() {
            let counters_dict = PyDict::new(py);
            for (name, value) in counters {
                counters_dict.set_item(name, value)?;
            }
            payload.set_item("counters", counters_dict)?;
        }
        Ok(payload.into())
    }

    pub fn reset(&self) {
        if let Ok(mut metrics) = self.inner.lock() {
            metrics.clear();
        }
    }
}

impl PyMetricsCollector {
    pub fn lock(&self) -> MutexGuard<'_, OperatorMetrics> {
        self.inner.lock().expect("Failed to lock metrics sink")
    }
}
