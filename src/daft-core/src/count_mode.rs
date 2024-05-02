#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes, PyTypeInfo};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use std::str::FromStr;

use crate::impl_bincode_py_state_serialization;

use common_error::{DaftError, DaftResult};

/// Supported count modes for Daft's count aggregation.
///
/// | All   - Count both non-null and null values.
/// | Valid - Count only valid values.
/// | Null  - Count only null values.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum CountMode {
    All = 1,
    Valid = 2,
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
    pub fn iterator() -> std::slice::Iter<'static, CountMode> {
        use CountMode::*;

        static COUNT_MODES: [CountMode; 3] = [All, Valid, Null];
        COUNT_MODES.iter()
    }
}

impl FromStr for CountMode {
    type Err = DaftError;

    fn from_str(count_mode: &str) -> DaftResult<Self> {
        use CountMode::*;

        match count_mode {
            "all" => Ok(All),
            "valid" => Ok(Valid),
            "null" => Ok(Null),
            _ => Err(DaftError::TypeError(format!(
                "Count mode {} is not supported; only the following modes are supported: {:?}",
                count_mode,
                CountMode::iterator().as_slice()
            ))),
        }
    }
}

impl Display for CountMode {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}
