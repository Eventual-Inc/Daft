use crate::datatypes::{DataType, Field, FixedShapeImageType, ImageFormat, ImageType};

use crate::series::{IntoSeries, Series};
use common_error::{DaftError, DaftResult};

impl Series {
    pub fn image_decode(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => Ok(self.binary()?.image_decode()?.into_series()),
            dtype => Err(DaftError::ValueError(format!(
                "Decoding in-memory data into images is only supported for binary arrays, but got {}", dtype
            ))),
        }
    }

    pub fn image_encode(&self, image_format: ImageFormat) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Image(..) => Ok(self
                .downcast_logical::<ImageType>()?
                .encode(image_format)?
                .into_series()),
            DataType::FixedShapeImage(..) => Ok(self
                .downcast_logical::<FixedShapeImageType>()?
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
                let array = self.downcast_logical::<ImageType>()?;
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
                .downcast_logical::<FixedShapeImageType>()?
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
        let bbox_type = DataType::FixedSizeList(Box::new(Field::new("bbox", DataType::Float64)), 4);
        let bbox = match &bbox.data_type() {
            DataType::List(child) => bbox.cast(&bbox_type)?.fixed_size_list(),
            DataType::FixedSizeList(field, 4) => bbox.cast(&bbox_type)?.fixed_size_list(),
            dt => Err(DaftError::ValueError(format!(
                "Expected bbox for crop to be a list of size 4, but received instead: {}",
                dt
            ))),
        }?;

        match &self.data_type() {
            DataType::Image(_) => self.image()?.crop(bbox),
            DataType::FixedShapeImage(..) => self.fixed_size_image()?.crop(bbox),
            dt => Err(DaftError::ValueError(format!(
                "Expected input to crop to be an Image type, but received: {}",
                dt
            ))),
        }
    }
}
