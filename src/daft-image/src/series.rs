use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use crate::{
    ops::{image_array_from_img_buffers, ImageOps},
    DaftImageBuffer,
};
fn image_decode_impl(
    ba: &BinaryArray,
    raise_error_on_failure: bool,
    mode: Option<ImageMode>,
) -> DaftResult<ImageArray> {
    let arrow_array = ba
        .data()
        .as_any()
        .downcast_ref::<arrow2::array::BinaryArray<i64>>()
        .unwrap();
    let mut img_bufs = Vec::<Option<DaftImageBuffer>>::with_capacity(arrow_array.len());
    let mut cached_dtype: Option<DataType> = None;
    // Load images from binary buffers.
    // Confirm that all images have the same value dtype.
    for (index, row) in arrow_array.iter().enumerate() {
        let mut img_buf = match row.map(DaftImageBuffer::decode).transpose() {
            Ok(val) => val,
            Err(err) => {
                if raise_error_on_failure {
                    return Err(err);
                }
                log::warn!(
                        "Error occurred during image decoding at index: {index} {} (falling back to Null)",
                        err
                    );
                None
            }
        };
        if let Some(mode) = mode {
            img_buf = img_buf.map(|buf| buf.into_mode(mode));
        }
        let dtype = img_buf.as_ref().map(|im| im.mode().get_dtype());
        match (dtype.as_ref(), cached_dtype.as_ref()) {
            (Some(t1), Some(t2)) => {
                if t1 != t2 {
                    return Err(DaftError::ValueError(format!(
                        "All images in a column must have the same dtype, but got: {t1:?} and {t2:?}"
                    )));
                }
            }
            (Some(t1), None) => {
                cached_dtype = Some(t1.clone());
            }
            (None, _) => {}
        }
        img_bufs.push(img_buf);
    }
    // Fall back to UInt8 dtype if series is all nulls.
    let cached_dtype = cached_dtype.unwrap_or(DataType::UInt8);
    match cached_dtype {
        DataType::UInt8 => Ok(image_array_from_img_buffers(ba.name(), img_bufs.as_slice(), mode)?),
        _ => unimplemented!("Decoding images of dtype {cached_dtype:?} is not supported, only uint8 images are supported."),
    }
}

/// Decodes a series of binary data into image arrays.
///
/// # Arguments
/// * `s` - Input Series containing binary image data
/// * `raise_error_on_failure` - If true, raises errors on decode failures
/// * `mode` - Optional target ImageMode for decoded images
///
/// # Returns
/// A DaftResult containing a Series of decoded images
pub fn decode(
    s: &Series,
    raise_error_on_failure: bool,
    mode: Option<ImageMode>,
) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Binary => image_decode_impl(s.binary()?, raise_error_on_failure, mode)
            .map(|arr| arr.into_series()),
        dtype => Err(DaftError::ValueError(format!(
            "Decoding in-memory data into images is only supported for binary arrays, but got {dtype}"
        ))),
    }
}

/// Encode a series of images into a series of bytes
/// Encode a series of images into a series of bytes.
///
/// This function takes a Series containing image data and an ImageFormat,
/// then encodes each image into the specified format.
///
/// # Arguments
/// * `s` - The input Series containing image data
/// * `image_format` - The desired output format for the encoded images
///
/// # Returns
/// A DaftResult containing a new Series of encoded binary data
pub fn encode(s: &Series, image_format: ImageFormat) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Image(..) => Ok(s
            .downcast::<ImageArray>()?
            .encode(image_format)?
            .into_series()),
        DataType::FixedShapeImage(..) => Ok(s
            .downcast::<FixedShapeImageArray>()?
            .encode(image_format)?
            .into_series()),
        dtype => Err(DaftError::ValueError(format!(
            "Encoding images into bytes is only supported for image arrays, but got {dtype}"
        ))),
    }
}
/// Resizes images in a Series to the specified width and height.
///
/// # Arguments
/// * `s` - Input Series containing image data
/// * `w` - Target width for resized images
/// * `h` - Target height for resized images
///
/// # Returns
/// A DaftResult containing a new Series with resized images
pub fn resize(s: &Series, w: u32, h: u32) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Image(mode) => {
            let array = s.downcast::<ImageArray>()?;
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
        DataType::FixedShapeImage(..) => Ok(s
            .downcast::<FixedShapeImageArray>()?
            .resize(w, h)?
            .into_series()),
        _ => Err(DaftError::ValueError(format!(
            "datatype: {} does not support Image Resize. Occurred while resizing Series: {}",
            s.data_type(),
            s.name()
        ))),
    }
}

/// Crops images in a Series based on provided bounding boxes.
///
/// # Arguments
/// * `s` - Input Series containing image data
/// * `bbox` - Series of bounding boxes for cropping
///
/// # Returns
/// A DaftResult containing a new Series with cropped images
pub fn crop(s: &Series, bbox: &Series) -> DaftResult<Series> {
    let bbox_type = DataType::FixedSizeList(Box::new(DataType::UInt32), 4);
    let bbox = bbox.cast(&bbox_type)?;
    let bbox = bbox.fixed_size_list()?;

    match &s.data_type() {
        DataType::Image(_) => s
            .downcast::<ImageArray>()?
            .crop(bbox)
            .map(|arr| arr.into_series()),

        DataType::FixedShapeImage(..) => s
            .fixed_size_image()?
            .crop(bbox)
            .map(|arr| arr.into_series()),

        dt => Err(DaftError::ValueError(format!(
            "Expected input to crop to be an Image type, but received: {dt}"
        ))),
    }
}
/// Converts images in a Series to the specified mode.
///
/// # Arguments
/// * `s` - Input Series containing image data
/// * `mode` - Target ImageMode for conversion
///
/// # Returns
/// A DaftResult containing a new Series with converted images
pub fn to_mode(s: &Series, mode: ImageMode) -> DaftResult<Series> {
    match &s.data_type() {
        DataType::Image(_) => s
            .downcast::<ImageArray>()?
            .to_mode(mode)
            .map(|arr| arr.into_series()),
        DataType::FixedShapeImage(..) => s
            .fixed_size_image()?
            .to_mode(mode)
            .map(|arr| arr.into_series()),
        dt => Err(DaftError::ValueError(format!(
            "Expected input to crop to be an Image type, but received: {dt}"
        ))),
    }
}
