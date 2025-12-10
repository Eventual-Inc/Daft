use std::collections::HashMap;

use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::{
    Bound, IntoPyObject, PyAny, PyResult, Python, pyclass, pymethods,
    types::{PyDict, PyDictMethods, PyList, PyListMethods},
};
use serde::{Deserialize, Serialize};

use crate::{Stat, operator_metrics::OperatorMetrics};

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

    pub fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = PyDict::new(py);
        for (name, counters) in self.inner.snapshot() {
            let counter_list = PyList::empty(py);
            for counter in counters {
                let counter_entry = PyDict::new(py);
                counter_entry.set_item("value", counter.value)?;
                match &counter.description {
                    Some(description) => counter_entry.set_item("description", description)?,
                    None => counter_entry.set_item("description", py.None())?,
                }
                counter_entry.set_item("attributes", counter.attributes.clone())?;
                counter_list.append(counter_entry)?;
            }
            result.set_item(name, counter_list)?;
        }
        Ok(result.into_any())
    }
}

impl_bincode_py_state_serialization!(PyOperatorMetrics);
