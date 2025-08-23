use std::str::FromStr;

use common_error::DaftError;
use common_py_serde::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum ImageProperty {
    Height,
    Width,
    Channel,
    Mode,
}

#[cfg(feature = "python")]
#[pymethods]
impl ImageProperty {
    /// Create an ImageProperty from its string representation.
    ///
    /// Args:
    ///     attr: String representation of the property. This is the same as the enum
    ///         attribute name, e.g. ``ImageProperty.from_property_string("channel")`` would
    ///         return ``ImageProperty.Channel``.
    #[staticmethod]
    pub fn from_property_string(attr: &str) -> PyResult<Self> {
        Self::from_str(attr)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl std::fmt::Display for ImageProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Height => write!(f, "height"),
            Self::Width => write!(f, "width"),
            Self::Channel => write!(f, "channel"),
            Self::Mode => write!(f, "mode"),
        }
    }
}

impl FromStr for ImageProperty {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "height" => Ok(Self::Height),
            "width" => Ok(Self::Width),
            "channel" => Ok(Self::Channel),
            "mode" => Ok(Self::Mode),
            _ => Err(DaftError::ValueError(format!(
                "Unsupported image property: {}, available: [height, width, channel, mode]",
                s
            ))),
        }
    }
}

impl_bincode_py_state_serialization!(ImageProperty);
