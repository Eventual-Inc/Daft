use std::str::FromStr;

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

/// Supported image formats for Daft's I/O layer.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum ImageFormat {
    PNG,
    JPEG,
    TIFF,
    GIF,
    BMP,
}

#[cfg(feature = "python")]
#[pymethods]
impl ImageFormat {
    /// Create an ImageFormat from its string representation.
    ///
    /// Args:
    ///     mode: String representation of the image format. This is the same as the enum
    ///         attribute name, e.g. ``ImageFormat.from_mode_string("JPEG")`` would
    ///         return ``ImageFormat.JPEG``.
    #[staticmethod]
    pub fn from_format_string(format: &str) -> PyResult<Self> {
        Self::from_str(format).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl ImageFormat {
    pub fn iterator() -> std::slice::Iter<'static, Self> {
        use ImageFormat::{BMP, GIF, JPEG, PNG, TIFF};

        static FORMATS: [ImageFormat; 5] = [PNG, JPEG, TIFF, GIF, BMP];
        FORMATS.iter()
    }
}

impl FromStr for ImageFormat {
    type Err = DaftError;

    fn from_str(format: &str) -> DaftResult<Self> {
        use ImageFormat::{BMP, GIF, JPEG, PNG, TIFF};

        match format {
            "PNG" => Ok(PNG),
            "JPEG" => Ok(JPEG),
            "TIFF" => Ok(TIFF),
            "GIF" => Ok(GIF),
            "BMP" => Ok(BMP),
            _ => Err(DaftError::TypeError(format!(
                "Image format {} is not supported; only the following formats are supported: {:?}",
                format,
                Self::iterator().as_slice()
            ))),
        }
    }
}

impl_bincode_py_state_serialization!(ImageFormat);
