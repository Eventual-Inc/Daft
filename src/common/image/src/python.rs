use daft_schema::prelude::ImageMode;
use image::{DynamicImage, ImageBuffer};
use pyo3::{intern, prelude::*};

use crate::Image;

impl<'py> FromPyObject<'_, 'py> for Image {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let py = ob.py();
        let mode: String = ob.getattr(intern!(py, "mode"))?.extract()?;
        let width: u32 = ob.getattr(intern!(py, "width"))?.extract()?;
        let height: u32 = ob.getattr(intern!(py, "height"))?.extract()?;
        let buf: Vec<u8> = ob.call_method0(intern!(py, "tobytes"))?.extract()?;

        Ok(match ImageMode::from_pil_mode_str(&mode)? {
            ImageMode::L => Self(DynamicImage::ImageLuma8(
                ImageBuffer::from_raw(width, height, buf).unwrap(),
            )),
            ImageMode::LA => Self(DynamicImage::ImageLumaA8(
                ImageBuffer::from_raw(width, height, buf).unwrap(),
            )),
            ImageMode::RGB => Self(DynamicImage::ImageRgb8(
                ImageBuffer::from_raw(width, height, buf).unwrap(),
            )),
            ImageMode::RGBA => Self(DynamicImage::ImageRgba8(
                ImageBuffer::from_raw(width, height, buf).unwrap(),
            )),
            _ => unimplemented!("PIL Image to Daft with mode {mode} not yet implemented."),
        })
    }
}
