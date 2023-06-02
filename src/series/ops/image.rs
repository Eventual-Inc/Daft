use arrow2;

use crate::datatypes::{DataType, ImageType, NullArray};

use crate::{
    error::{DaftError, DaftResult},
    series::{IntoSeries, Series},
};

impl Series {
    pub fn image_decode(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => {
                let binary_array = self.binary()?;
                if binary_array.data().null_count() == self.len() {
                    // All images are None, so return a NullArray.
                    Ok(NullArray::from(("item", Box::new(arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, self.len())))).into_series())
                } else {
                    Ok(self.binary()?.image_decode()?.into_series())
                }
            },
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
