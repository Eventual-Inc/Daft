use std::{
    fmt::{Display, Formatter, Result},
    str::FromStr,
};

use common_error::{DaftError, DaftResult};
use daft_core::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
    Python, ToPyObject,
};

use serde::{Deserialize, Serialize};

/// Type of a join operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinType {
    /// Create a JoinType from its string representation.
    ///
    /// Args:
    ///     join_type: String representation of the join type, e.g. "inner", "left", or "right".
    #[staticmethod]
    pub fn from_join_type_str(join_type: &str) -> PyResult<Self> {
        Self::from_str(join_type).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(JoinType);

impl JoinType {
    pub fn iterator() -> std::slice::Iter<'static, JoinType> {
        use JoinType::*;

        static JOIN_TYPES: [JoinType; 3] = [Inner, Left, Right];
        JOIN_TYPES.iter()
    }
}

impl FromStr for JoinType {
    type Err = DaftError;

    fn from_str(join_type: &str) -> DaftResult<Self> {
        use JoinType::*;

        match join_type {
            "inner" => Ok(Inner),
            "left" => Ok(Left),
            "right" => Ok(Right),
            _ => Err(DaftError::TypeError(format!(
                "Join type {} is not supported; only the following modes are supported: {:?}",
                join_type,
                JoinType::iterator().as_slice()
            ))),
        }
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}
