use crate::datatypes::DataType;
use num_derive::FromPrimitive;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use std::str::FromStr;

use common_error::{DaftError, DaftResult};

/// Supported image modes for Daft's image type.
///
/// .. warning::
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash, FromPrimitive)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
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
        use ImageMode::*;

        match mode {
            "L" => Ok(L),
            "LA" => Ok(LA),
            "RGB" => Ok(RGB),
            "RGBA" => Ok(RGBA),
            "1" | "P" | "CMYK" | "YCbCr" | "LAB" | "HSV" | "I" | "F" | "PA" | "RGBX" | "RGBa" | "La" | "I;16" | "I;16L" | "I;16B" | "I;16N" | "BGR;15" | "BGR;16" | "BGR;24" => Err(DaftError::TypeError(format!(
                "PIL image mode {} is not supported; only the following modes are supported: {:?}",
                mode,
                ImageMode::iterator().as_slice()
            ))),
            _ => Err(DaftError::TypeError(format!(
                "Image mode {} is not a valid PIL image mode; see https://pillow.readthedocs.io/en/stable/handbook/concepts.html#modes for valid PIL image modes. Of these, only the following modes are supported by Daft: {:?}",
                mode,
                ImageMode::iterator().as_slice()
            ))),
        }
    }
    pub fn try_from_num_channels(num_channels: u16, dtype: &DataType) -> DaftResult<Self> {
        use ImageMode::*;

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
                "Images with more than {} channels and dtype {} are not supported",
                num_channels, dtype,
            ))),
        }
    }
    pub fn num_channels(&self) -> u16 {
        use ImageMode::*;

        match self {
            L | L16 => 1,
            LA | LA16 => 2,
            RGB | RGB16 | RGB32F => 3,
            RGBA | RGBA16 | RGBA32F => 4,
        }
    }
    pub fn iterator() -> std::slice::Iter<'static, ImageMode> {
        use ImageMode::*;

        static MODES: [ImageMode; 10] =
            [L, LA, RGB, RGBA, L16, LA16, RGB16, RGBA16, RGB32F, RGBA32F];
        MODES.iter()
    }
    pub fn get_dtype(&self) -> DataType {
        self.into()
    }
}

impl From<ImageMode> for image::ColorType {
    fn from(image_mode: ImageMode) -> image::ColorType {
        use image::ColorType;
        use ImageMode::*;

        match image_mode {
            L => ColorType::L8,
            LA => ColorType::La8,
            RGB => ColorType::Rgb8,
            RGBA => ColorType::Rgba8,
            L16 => ColorType::L16,
            LA16 => ColorType::La16,
            RGB16 => ColorType::Rgb16,
            RGBA16 => ColorType::Rgba16,
            RGB32F => ColorType::Rgb32F,
            RGBA32F => ColorType::Rgba32F,
        }
    }
}

impl TryFrom<image::ColorType> for ImageMode {
    type Error = DaftError;

    fn try_from(color: image::ColorType) -> DaftResult<Self> {
        use image::ColorType;
        use ImageMode::*;

        match color {
            ColorType::L8 => Ok(L),
            ColorType::La8 => Ok(LA),
            ColorType::Rgb8 => Ok(RGB),
            ColorType::Rgba8 => Ok(RGBA),
            ColorType::L16 => Ok(L16),
            ColorType::La16 => Ok(LA16),
            ColorType::Rgb16 => Ok(RGB16),
            ColorType::Rgba16 => Ok(RGBA16),
            ColorType::Rgb32F => Ok(RGB32F),
            ColorType::Rgba32F => Ok(RGBA32F),
            _ => Err(DaftError::ValueError(format!(
                "Color type {:?} is not supported.",
                color
            ))),
        }
    }
}

impl FromStr for ImageMode {
    type Err = DaftError;

    fn from_str(mode: &str) -> DaftResult<Self> {
        use ImageMode::*;

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
                ImageMode::iterator().as_slice()
            ))),
        }
    }
}

impl Display for ImageMode {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}
