use arrow2;
use image;

use crate::datatypes::{logical::ImageArray, DataType, ImageMode, ImageType, NullArray};

use crate::with_match_numeric_daft_types;
use crate::{
    error::{DaftError, DaftResult},
    series::{IntoSeries, Series},
};

impl Series {
    pub fn image_decode(&self) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => {
                let arrow_array = self.to_arrow();
                let arrow_array = arrow_array.as_any().downcast_ref::<arrow2::array::BinaryArray<i64>>().unwrap();
                let num_rows = arrow_array.len();
                let mut dyn_imgs = Vec::<Option<image::DynamicImage>>::with_capacity(num_rows);
                let mut cached_dtype: Option<DataType> = None;
                // Load images from binary buffers.
                // Confirm that all images have the same value dtype.
                for row in arrow_array.iter() {
                    let dyn_img = row.map(|buf| image::load_from_memory(buf)).transpose().map_err(|e| DaftError::ValueError(format!("Decoding image from bytes failed: {}", e)))?;
                    let dtype = dyn_img.as_ref().map(|img| ImageMode::try_from(&img.color()).unwrap().to_dtype());
                    match (dtype.as_ref(), cached_dtype.as_ref()) {
                        (Some(t1), Some(t2)) => {
                            if t1 != t2 {
                                return Err(DaftError::ValueError(format!("All images in a column must have the same dtype, but got: {:?} and {:?}", t1, t2)));
                            }
                        }
                        (Some(t1), None) => {
                            cached_dtype = Some(t1.clone());
                        }
                        (None, _) => {}
                    }
                    dyn_imgs.push(dyn_img);
                }
                match cached_dtype {
                    Some(dtype) => {
                        with_match_numeric_daft_types!(dtype, |$T| {
                            type Tgt = <$T as DaftNumericType>::Native;
                            let result = ImageArray::from_dyn_images::<Tgt>(self.name(), dyn_imgs, num_rows, &dtype)?;
                            Ok(result.into_series())
                        })
                    }
                    // All images are None, so return a NullArray.
                    None => Ok(NullArray::from(("item", Box::new(arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, num_rows)))).into_series()),
                }
            },
            dt => Err(DaftError::ValueError(format!(
                "Decoding in-memory data into images is only supported for binary arrays, but got {}", dt
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
