use daft_dsl::Expr;

use daft_core::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{
        pyclass, pyclass::CompareOp, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
};

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum PartitionScheme {
    Range,
    Hash,
    Random,
    Unknown,
}

impl_bincode_py_state_serialization!(PartitionScheme);

/// Partition specification: scheme, number of partitions, partition column.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct PartitionSpec {
    pub scheme: PartitionScheme,
    pub num_partitions: usize,
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

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(PartitionSpec);
