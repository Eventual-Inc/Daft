use crate::datatypes::{DataType, ImageType};

use crate::{
    error::{DaftError, DaftResult},
    series::{IntoSeries, Series},
};

impl Series {
    pub fn image_decode(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => Ok(self.binary()?.image_decode()?.into_series()),
            dtype => Err(DaftError::ValueError(format!(
                "Decoding in-memory data into images is only supported for binary arrays, but got {}", dtype
            ))),
        }
    }

    pub fn image_resize(&self, w: u32, h: u32) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Image(..) => Ok(self
                .downcast_logical::<ImageType>()?
                .resize(w, h)?
                .into_series()),
            _ => Err(DaftError::ValueError(format!(
                "datatype: {} does not support Image Resize. Occurred while resizing Series: {}",
                self.data_type(),
                self.name()
            ))),
        }
    }
}
