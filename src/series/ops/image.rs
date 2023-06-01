use arrow2;
use image;

use crate::array::ops::image::DaftImageBuffer;
use crate::datatypes::{logical::ImageArray, DataType, ImageType, NullArray};

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
                let mut img_bufs = Vec::<Option<DaftImageBuffer>>::with_capacity(arrow_array.len());
                let mut cached_dtype: Option<DataType> = None;
                // Load images from binary buffers.
                // Confirm that all images have the same value dtype.
                for row in arrow_array.iter() {
                    let dyn_img = row.map(image::load_from_memory).transpose().map_err(|e| DaftError::ValueError(format!("Decoding image from bytes failed: {}", e)))?;
                    let img_buf: Option<DaftImageBuffer> = dyn_img.map(|im| im.into());
                    let dtype = img_buf.as_ref().map(|im| im.mode().get_dtype());
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
                    img_bufs.push(img_buf);
                }
                match cached_dtype {
                    Some(dtype) => match dtype {
                        DataType::UInt8 => Ok(ImageArray::from_daft_image_buffers(self.name(), img_bufs.as_slice(), &None)?.into_series()),
                        _ => unimplemented!("Decoding images of dtype {dtype:?} is not supported, only uint8 images are supported."),
                    }
                    // All images are None, so return a NullArray.
                    None => Ok(NullArray::from(("item", Box::new(arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, img_bufs.len())))).into_series()),
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
