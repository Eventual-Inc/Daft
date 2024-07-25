use std::sync::Arc;

use crate::array::ops::from_arrow::FromArrow;
use crate::datatypes::logical::{FixedShapeImageArray, ImageArray};
use crate::datatypes::{
    DataType, Field, ImageFormat, ImageMode, UInt16Array, UInt32Array, Utf8Array,
};

use crate::series::{IntoSeries, Series};
use common_error::{DaftError, DaftResult};
use num_traits::FromPrimitive;

impl Series {
    pub fn image_decode(&self, raise_error_on_failure: bool) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => Ok(self.binary()?.image_decode(raise_error_on_failure)?.into_series()),
            dtype => Err(DaftError::ValueError(format!(
                "Decoding in-memory data into images is only supported for binary arrays, but got {}", dtype
            ))),
        }
    }

    pub fn image_encode(&self, image_format: ImageFormat) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Image(..) => Ok(self
                .downcast::<ImageArray>()?
                .encode(image_format)?
                .into_series()),
            DataType::FixedShapeImage(..) => Ok(self
                .downcast::<FixedShapeImageArray>()?
                .encode(image_format)?
                .into_series()),
            dtype => Err(DaftError::ValueError(format!(
                "Encoding images into bytes is only supported for image arrays, but got {}",
                dtype
            ))),
        }
    }

    pub fn image_resize(&self, w: u32, h: u32) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Image(mode) => {
                let array = self.downcast::<ImageArray>()?;
                match mode {
                    // If the image mode is specified at the type-level (and is therefore guaranteed to be consistent
                    // across all images across all partitions), store the resized image in a fixed shape image array,
                    // since we'll have homogeneous modes, heights, and widths after resizing.
                    Some(mode) => Ok(array
                        .resize_to_fixed_shape_image_array(w, h, mode)?
                        .into_series()),
                    None => Ok(array.resize(w, h)?.into_series()),
                }
            }
            DataType::FixedShapeImage(..) => Ok(self
                .downcast::<FixedShapeImageArray>()?
                .resize(w, h)?
                .into_series()),
            _ => Err(DaftError::ValueError(format!(
                "datatype: {} does not support Image Resize. Occurred while resizing Series: {}",
                self.data_type(),
                self.name()
            ))),
        }
    }

    pub fn image_crop(&self, bbox: &Series) -> DaftResult<Series> {
        let bbox_type = DataType::FixedSizeList(Box::new(DataType::UInt32), 4);
        let bbox = bbox.cast(&bbox_type)?;
        let bbox = bbox.fixed_size_list()?;

        match &self.data_type() {
            DataType::Image(_) => self
                .downcast::<ImageArray>()?
                .crop(bbox)
                .map(|arr| arr.into_series()),
            DataType::FixedShapeImage(..) => self
                .fixed_size_image()?
                .crop(bbox)
                .map(|arr| arr.into_series()),
            dt => Err(DaftError::ValueError(format!(
                "Expected input to crop to be an Image type, but received: {}",
                dt
            ))),
        }
    }

    pub fn image_width(&self) -> DaftResult<Series> {
        match &self.data_type() {
            DataType::Image(_) => Ok(UInt32Array::from_arrow(
                Arc::new(Field::new(self.name(), DataType::UInt32)),
                self.downcast::<ImageArray>()?.width_array().clone().boxed(),
            )?
            .into_series()),
            DataType::FixedShapeImage(_mode, _height, width) => Ok(UInt32Array::from((
                self.name(),
                std::iter::repeat(*width)
                    .take(self.len())
                    .collect::<Vec<u32>>(),
            ))
            .with_validity(self.validity().cloned())?
            .into_series()),
            dt => Err(DaftError::ValueError(format!(
                "Expected input to width to be an Image type, but received: {}",
                dt
            ))),
        }
    }

    pub fn image_height(&self) -> DaftResult<Series> {
        match &self.data_type() {
            DataType::Image(_) => Ok(UInt32Array::from_arrow(
                Arc::new(Field::new(self.name(), DataType::UInt32)),
                self.downcast::<ImageArray>()?
                    .height_array()
                    .clone()
                    .boxed(),
            )?
            .into_series()),
            DataType::FixedShapeImage(_mode, height, _width) => Ok(UInt32Array::from((
                self.name(),
                std::iter::repeat(*height)
                    .take(self.len())
                    .collect::<Vec<u32>>(),
            ))
            .with_validity(self.validity().cloned())?
            .into_series()),
            dt => Err(DaftError::ValueError(format!(
                "Expected input to height to be an Image type, but received: {}",
                dt
            ))),
        }
    }

    pub fn image_channels(&self) -> DaftResult<Series> {
        match &self.data_type() {
            DataType::Image(_) => Ok(UInt16Array::from_arrow(
                Arc::new(Field::new(self.name(), DataType::UInt16)),
                self.downcast::<ImageArray>()?
                    .channel_array()
                    .clone()
                    .boxed(),
            )?
            .into_series()),
            DataType::FixedShapeImage(mode, _height, _width) => Ok(UInt32Array::from((
                self.name(),
                std::iter::repeat(mode.num_channels() as u32)
                    .take(self.len())
                    .collect::<Vec<u32>>(),
            ))
            .with_validity(self.validity().cloned())?
            .into_series()),
            dt => Err(DaftError::ValueError(format!(
                "Expected input to mode to be an Image type, but received: {}",
                dt
            ))),
        }
    }

    pub fn image_mode(&self) -> DaftResult<Series> {
        match &self.data_type() {
            DataType::Image(_) => Ok(Utf8Array::from((
                self.name(),
                self.downcast::<ImageArray>()?
                    .mode_array()
                    .values_iter()
                    .map(|m| {
                        FromPrimitive::from_u8(*m)
                            .map_or("".to_string(), |m: ImageMode| m.to_string())
                    })
                    .collect::<Vec<String>>()
                    .as_slice(),
            ))
            .with_validity(self.validity().cloned())?
            .into_series()),
            DataType::FixedShapeImage(mode, _height, _width) => {
                let mode_string_vec = std::iter::repeat(mode.to_string())
                    .take(self.len())
                    .collect::<Vec<_>>();
                Ok(Utf8Array::from((self.name(), mode_string_vec.as_slice()))
                    .with_validity(self.validity().cloned())?
                    .into_series())
            }
            dt => Err(DaftError::ValueError(format!(
                "Expected input to mode to be an Image type, but received: {}",
                dt
            ))),
        }
    }
}
