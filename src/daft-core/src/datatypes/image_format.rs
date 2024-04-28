use std::fmt::{Display, Formatter, Result};
use std::str::FromStr;

#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

use common_error::{DaftError, DaftResult};

/// Supported image formats for Daft's I/O layer.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
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
    pub fn iterator() -> std::slice::Iter<'static, ImageFormat> {
        use ImageFormat::*;

        static FORMATS: [ImageFormat; 5] = [PNG, JPEG, TIFF, GIF, BMP];
        FORMATS.iter()
    }
}

impl FromStr for ImageFormat {
    type Err = DaftError;

    fn from_str(format: &str) -> DaftResult<Self> {
        use ImageFormat::*;

        match format {
            "PNG" => Ok(PNG),
            "JPEG" => Ok(JPEG),
            "TIFF" => Ok(TIFF),
            "GIF" => Ok(GIF),
            "BMP" => Ok(BMP),
            _ => Err(DaftError::TypeError(format!(
                "Image format {} is not supported; only the following formats are supported: {:?}",
                format,
                ImageFormat::iterator().as_slice()
            ))),
        }
    }
}

impl From<image::ImageFormat> for ImageFormat {
    fn from(image_format: image::ImageFormat) -> Self {
        match image_format {
            image::ImageFormat::Png => ImageFormat::PNG,
            image::ImageFormat::Jpeg => ImageFormat::JPEG,
            image::ImageFormat::Tiff => ImageFormat::TIFF,
            image::ImageFormat::Gif => ImageFormat::GIF,
            image::ImageFormat::Bmp => ImageFormat::BMP,
            _ => unimplemented!("Image format {:?} is not supported", image_format),
        }
    }
}

impl From<ImageFormat> for image::ImageFormat {
    fn from(image_format: ImageFormat) -> Self {
        match image_format {
            ImageFormat::PNG => image::ImageFormat::Png,
            ImageFormat::JPEG => image::ImageFormat::Jpeg,
            ImageFormat::TIFF => image::ImageFormat::Tiff,
            ImageFormat::GIF => image::ImageFormat::Gif,
            ImageFormat::BMP => image::ImageFormat::Bmp,
        }
    }
}

impl Display for ImageFormat {
    fn fmt(&self, f: &mut Formatter) -> Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}
