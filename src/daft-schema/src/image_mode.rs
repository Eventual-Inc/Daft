use std::str::FromStr;

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
use num_derive::FromPrimitive;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

use crate::dtype::DataType;

/// Supported image modes for Daft's image type.
///
/// Warning:
///     Currently, only the 8-bit modes (L, LA, RGB, RGBA) can be stored in a DataFrame.
///     If your binary image data includes other modes, use the `mode` argument
///     in `image.decode` to convert the images to a supported mode.
///
/// | L       - 8-bit grayscale
/// | LA      - 8-bit grayscale + alpha
/// | RGB     - 8-bit RGB
/// | RGBA    - 8-bit RGB + alpha
/// | L16     - 16-bit grayscale
/// | LA16    - 16-bit grayscale + alpha
/// | RGB16   - 16-bit RGB
/// | RGBA16  - 16-bit RGB + alpha
/// | RGB32F  - 32-bit floating RGB
/// | RGBA32F - 32-bit floating RGB + alpha
#[allow(clippy::upper_case_acronyms)]
#[derive(
    Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash, FromPrimitive,
)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum ImageMode {
    L = 1,
    LA = 2,
    RGB = 3,
    RGBA = 4,
    L16 = 5,
    LA16 = 6,
    RGB16 = 7,
    RGBA16 = 8,
    RGB32F = 9,
    RGBA32F = 10,
}

#[cfg(feature = "python")]
#[pymethods]
impl ImageMode {
    /// Create an ImageMode from its string representation.
    ///
    /// Args:
    ///     mode: String representation of the mode. This is the same as the enum
    ///         attribute name, e.g. ``ImageMode.from_mode_string("RGB")`` would
    ///         return ``ImageMode.RGB``.
    #[staticmethod]
    pub fn from_mode_string(mode: &str) -> PyResult<Self> {
        Self::from_str(mode).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}

impl ImageMode {
    pub fn from_pil_mode_str(mode: &str) -> DaftResult<Self> {
        use ImageMode::{L, LA, RGB, RGBA};

        match mode {
            "L" => Ok(L),
            "LA" => Ok(LA),
            "RGB" => Ok(RGB),
            "RGBA" => Ok(RGBA),
            "1" | "P" | "CMYK" | "YCbCr" | "LAB" | "HSV" | "I" | "F" | "PA" | "RGBX" | "RGBa" | "La" | "I;16" | "I;16L" | "I;16B" | "I;16N" | "BGR;15" | "BGR;16" | "BGR;24" => Err(DaftError::TypeError(format!(
                "PIL image mode {} is not supported; only the following modes are supported: {:?}",
                mode,
                Self::iterator().as_slice()
            ))),
            _ => Err(DaftError::TypeError(format!(
                "Image mode {} is not a valid PIL image mode; see https://pillow.readthedocs.io/en/stable/handbook/concepts.html#modes for valid PIL image modes. Of these, only the following modes are supported by Daft: {:?}",
                mode,
                Self::iterator().as_slice()
            ))),
        }
    }
    pub fn try_from_num_channels(num_channels: u16, dtype: &DataType) -> DaftResult<Self> {
        use ImageMode::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

        match (num_channels, dtype) {
            (1, DataType::UInt8) => Ok(L),
            (1, DataType::UInt16) => Ok(L16),
            (2, DataType::UInt8) => Ok(LA),
            (2, DataType::UInt16) => Ok(LA16),
            (3, DataType::UInt8) => Ok(RGB),
            (3, DataType::UInt16) => Ok(RGB16),
            (3, DataType::Float32) => Ok(RGB32F),
            (4, DataType::UInt8) => Ok(RGBA),
            (4, DataType::UInt16) => Ok(RGBA16),
            (4, DataType::Float32) => Ok(RGBA32F),
            (_, _) => Err(DaftError::ValueError(format!(
                "Images with more than {num_channels} channels and dtype {dtype} are not supported",
            ))),
        }
    }
    #[must_use]
    pub fn num_channels(&self) -> u16 {
        use ImageMode::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

        match self {
            L | L16 => 1,
            LA | LA16 => 2,
            RGB | RGB16 | RGB32F => 3,
            RGBA | RGBA16 | RGBA32F => 4,
        }
    }
    pub fn iterator() -> std::slice::Iter<'static, Self> {
        use ImageMode::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

        static MODES: [ImageMode; 10] =
            [L, LA, RGB, RGBA, L16, LA16, RGB16, RGBA16, RGB32F, RGBA32F];
        MODES.iter()
    }
    #[must_use]
    pub fn get_dtype(&self) -> DataType {
        self.into()
    }
}

impl FromStr for ImageMode {
    type Err = DaftError;

    fn from_str(mode: &str) -> DaftResult<Self> {
        use ImageMode::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

        match mode {
            "L" => Ok(L),
            "LA" => Ok(LA),
            "RGB" => Ok(RGB),
            "RGBA" => Ok(RGBA),
            "L16" => Ok(L16),
            "LA16" => Ok(LA16),
            "RGB16" => Ok(RGB16),
            "RGBA16" => Ok(RGBA16),
            "RGB32F" => Ok(RGB32F),
            "RGBA32F" => Ok(RGBA32F),
            _ => Err(DaftError::TypeError(format!(
                "Image mode {} is not supported; only the following modes are supported: {:?}",
                mode,
                Self::iterator().as_slice()
            ))),
        }
    }
}

impl_bincode_py_state_serialization!(ImageMode);
