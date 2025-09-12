use pyo3::{Bound, IntoPyObject, PyAny, PyResult, Python, pyclass};

use crate::Stat;

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
