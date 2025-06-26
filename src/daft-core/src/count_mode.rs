use std::str::FromStr;

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

/// Supported count modes for Daft's count aggregation.
///
/// | All   - Count both non-null and null values.
/// | Valid - Count only valid values.
/// | Null  - Count only null values.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum CountMode {
    #[serde(alias = "all")]
    All = 1,
    #[serde(alias = "valid")]
    Valid = 2,
    #[serde(alias = "null")]
    Null = 3,
}

#[cfg(feature = "python")]
#[pymethods]
impl CountMode {
    /// Create a CountMode from its string representation.
    ///
    /// Args:
    ///     count_mode: String representation of the count mode , e.g. "all", "valid", or "null".
    #[staticmethod]
    pub fn from_count_mode_str(count_mode: &str) -> PyResult<Self> {
        Self::from_str(count_mode).map_err(|e| PyValueError::new_err(e.to_string()))
    }
    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl_bincode_py_state_serialization!(CountMode);

impl CountMode {
    pub fn iterator() -> std::slice::Iter<'static, Self> {
        static COUNT_MODES: [CountMode; 3] = [CountMode::All, CountMode::Valid, CountMode::Null];
        COUNT_MODES.iter()
    }
}

impl FromStr for CountMode {
    type Err = DaftError;

    fn from_str(count_mode: &str) -> DaftResult<Self> {
        match count_mode.to_lowercase().as_str() {
            "all" => Ok(Self::All),
            "valid" => Ok(Self::Valid),
            "null" => Ok(Self::Null),
            _ => Err(DaftError::TypeError(format!(
                "Count mode {} is not supported; only the following modes are supported: {:?}",
                count_mode,
                Self::iterator().as_slice()
            ))),
        }
    }
}
