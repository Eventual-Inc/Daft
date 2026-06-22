use std::str::FromStr;

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", eq, eq_int, from_py_object)
)]
pub enum UnionMode {
    Sparse = 1,
    Dense = 2,
}

#[cfg(feature = "python")]
#[pymethods]
impl UnionMode {
    #[staticmethod]
    pub fn from_mode_string(mode: &str) -> pyo3::PyResult<Self> {
        Self::from_str(mode).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(UnionMode);

impl UnionMode {
    pub fn to_arrow(&self) -> arrow_schema::UnionMode {
        match self {
            Self::Sparse => arrow_schema::UnionMode::Sparse,
            Self::Dense => arrow_schema::UnionMode::Dense,
        }
    }

    pub fn is_dense(&self) -> bool {
        matches!(self, Self::Dense)
    }
}

impl FromStr for UnionMode {
    type Err = DaftError;

    fn from_str(mode: &str) -> DaftResult<Self> {
        match mode {
            "sparse" => Ok(Self::Sparse),
            "dense" => Ok(Self::Dense),
            _ => Err(DaftError::TypeError(format!(
                "Union mode {} is not supported; only sparse and dense mode are supported",
                mode,
            ))),
        }
    }
}
