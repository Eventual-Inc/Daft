use daft_dsl::Expr;

use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyclass, pymethods, types::PyBytes, PyResult, Python},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass)]
pub enum PartitionScheme {
    Range,
    Hash,
    Random,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass)]
pub struct PartitionSpec {
    #[pyo3(get)]
    pub scheme: PartitionScheme,
    #[pyo3(get)]
    pub num_partitions: usize,
    // TODO(Clark): Port ExpressionsProjection.
    pub by: Option<Vec<Expr>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PartitionSpec {
    #[new]
    pub fn new(scheme: PartitionScheme, num_partitions: usize, by: Option<Vec<PyExpr>>) -> Self {
        Self {
            scheme,
            num_partitions,
            by: by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
        }
    }

    #[getter]
    pub fn get_by(&self) -> PyResult<Option<Vec<PyExpr>>> {
        Ok(self
            .by
            .as_ref()
            .map(|v| v.iter().map(|e| e.clone().into()).collect()))
    }

    pub fn __setstate__(&mut self, state: &PyBytes) -> PyResult<()> {
        *self = bincode::deserialize(state.as_bytes()).unwrap();
        Ok(())
    }

    pub fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
        Ok(PyBytes::new(py, &bincode::serialize(&self).unwrap()))
    }
}
