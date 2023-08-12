use daft_dsl::Expr;

use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{
        exceptions::PyValueError, pyclass, pyclass::CompareOp, pymethods, types::PyBytes,
        types::PyTuple, PyResult, Python,
    },
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum PartitionScheme {
    Range,
    Hash,
    Random,
    Unknown,
}

#[cfg(feature = "python")]
#[pymethods]
impl PartitionScheme {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            // Create dummy variant, to be overridden by __setstate__.
            0 => Ok(Self::Unknown),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PartitionScheme, got : {}",
                args.len()
            ))),
        }
    }
}

impl_bincode_py_state_serialization!(PartitionScheme);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct PartitionSpec {
    pub scheme: PartitionScheme,
    pub num_partitions: usize,
    // TODO(Clark): Port ExpressionsProjection.
    pub by: Option<Vec<Expr>>,
}

impl PartitionSpec {
    pub fn new_internal(
        scheme: PartitionScheme,
        num_partitions: usize,
        by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            scheme,
            num_partitions,
            by,
        }
    }
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self {
            scheme: PartitionScheme::Unknown,
            num_partitions: 1,
            by: None,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PartitionSpec {
    #[new]
    #[pyo3(signature = (scheme=PartitionScheme::Unknown, num_partitions=0usize, by=None))]
    pub fn new(scheme: PartitionScheme, num_partitions: usize, by: Option<Vec<PyExpr>>) -> Self {
        Self::new_internal(
            scheme,
            num_partitions,
            by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
        )
    }

    #[getter]
    pub fn get_scheme(&self) -> PyResult<PartitionScheme> {
        Ok(self.scheme.clone())
    }

    #[getter]
    pub fn get_num_partitions(&self) -> PyResult<i64> {
        Ok(self.num_partitions as i64)
    }

    #[getter]
    pub fn get_by(&self) -> PyResult<Option<Vec<PyExpr>>> {
        Ok(self
            .by
            .as_ref()
            .map(|v| v.iter().map(|e| e.clone().into()).collect()))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl_bincode_py_state_serialization!(PartitionSpec);
