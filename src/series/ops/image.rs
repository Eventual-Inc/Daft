// use image;
// use ndarray;

use crate::datatypes::{DataType, ImageFormat};

use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

impl Series {
    pub fn image_decode(&self, _: Option<ImageFormat>) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Binary => {
                todo!("not implemented");
                // let arrow_array = self.to_arrow().as_any().downcast_ref::<arrow2::array::BinaryArray<i64>>()?;
                // let imgs: Vec<Box<dyn arrow2::array::Array>> = Vec::with_capacity(arrow_array.len());
                // for row in arrow_array.iter() {
                //     let dyn_img = row.map(|buf| {
                //         match image_format {
                //             Some(image_format) => {
                //                 let img_fmt = image::ImageFormat::from(image_format);
                //                 image::load_from_memory_with_format(buf, img_fmt)
                //             }
                //             None => image::load_from_memory(buf),
                //         }?;
                //     });
                //     imgs.push(dyn_image_to_arrow(dyn_img));
                // }
            },
            dt => Err(DaftError::ValueError(format!(
                "Decoding in-memory data into images is only supported for binary arrays, but got {}", dt
            ))),
        }
    }
}
