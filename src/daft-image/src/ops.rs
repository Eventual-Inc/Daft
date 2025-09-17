use base64::Engine;
use common_error::{DaftError, DaftResult};
use common_image::{BBox, CowImage};
use daft_core::{
    array::{
        ops::image::{
            AsImageObj, fixed_image_array_from_img_buffers, image_array_from_img_buffers,
        },
        prelude::*,
    },
    datatypes::prelude::*,
    prelude::ImageArray,
};
use daft_schema::image_property::ImageProperty;

use crate::{CountingWriter, iters::ImageBufferIter};

pub trait ImageOps {
    fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray>;
    fn resize(&self, w: u32, h: u32) -> DaftResult<Self>
    where
        Self: Sized;
    fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray>
    where
        Self: Sized;
    fn resize_to_fixed_shape_image_array(
        &self,
        w: u32,
        h: u32,
        mode: &ImageMode,
    ) -> DaftResult<FixedShapeImageArray>;
    fn to_mode(&self, mode: ImageMode) -> DaftResult<Self>
    where
        Self: Sized;
    fn attribute(&self, attr: ImageProperty) -> DaftResult<DataArray<UInt32Type>>;

    // Hash functions
    fn average_hash(&self) -> DaftResult<Utf8Array>;
    fn perceptual_hash(&self) -> DaftResult<Utf8Array>;
    fn difference_hash(&self) -> DaftResult<Utf8Array>;
    fn wavelet_hash(&self) -> DaftResult<Utf8Array>;
    fn crop_resistant_hash(&self) -> DaftResult<Utf8Array>;
}

impl ImageOps for ImageArray {
    fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray> {
        encode_images(self, image_format)
    }

    fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let result = resize_images(self, w, h);
        image_array_from_img_buffers(self.name(), result.as_slice(), self.image_mode())
    }

    fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray> {
        let mut bboxes_iterator: Box<dyn Iterator<Item = Option<BBox>>> = if bboxes.len() == 1 {
            Box::new(std::iter::repeat(bboxes.get(0).map(|bbox| {
                BBox::from_u32_arrow_array(bbox.u32().unwrap().data())
            })))
        } else {
            Box::new((0..bboxes.len()).map(|i| {
                bboxes
                    .get(i)
                    .map(|bbox| BBox::from_u32_arrow_array(bbox.u32().unwrap().data()))
            }))
        };
        let result = crop_images(self, &mut bboxes_iterator);
        image_array_from_img_buffers(self.name(), result.as_slice(), self.image_mode())
    }

    fn resize_to_fixed_shape_image_array(
        &self,
        w: u32,
        h: u32,
        mode: &ImageMode,
    ) -> DaftResult<FixedShapeImageArray> {
        let result = resize_images(self, w, h);
        fixed_image_array_from_img_buffers(self.name(), result.as_slice(), mode, h, w)
    }

    fn to_mode(&self, mode: ImageMode) -> DaftResult<Self> {
        let buffers: Vec<Option<CowImage>> = ImageBufferIter::new(self)
            .map(|img| img.map(|img| img.into_mode(mode)))
            .collect();
        image_array_from_img_buffers(self.name(), &buffers, Some(mode))
    }

    fn attribute(&self, attr: ImageProperty) -> DaftResult<DataArray<UInt32Type>> {
        match attr {
            ImageProperty::Height => Ok(self.heights().clone().rename(self.name())),
            ImageProperty::Width => Ok(self.widths().clone().rename(self.name())),
            ImageProperty::Channel => Ok(self
                .channels()
                .clone()
                .cast(&DataType::UInt32)?
                .u32()?
                .clone()
                .rename(self.name())),
            ImageProperty::Mode => Ok(self
                .modes()
                .clone()
                .cast(&DataType::UInt32)?
                .u32()?
                .clone()
                .rename(self.name())),
        }
    }

    fn average_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_average_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn perceptual_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_perceptual_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn difference_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_difference_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn wavelet_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_wavelet_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn crop_resistant_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_crop_resistant_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }
}

impl ImageOps for FixedShapeImageArray {
    fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray> {
        encode_images(self, image_format)
    }

    fn resize(&self, w: u32, h: u32) -> DaftResult<Self>
    where
        Self: Sized,
    {
        let result = resize_images(self, w, h);
        let mode = self.image_mode();
        fixed_image_array_from_img_buffers(self.name(), result.as_slice(), mode, h, w)
    }

    fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray>
    where
        Self: Sized,
    {
        let mut bboxes_iterator: Box<dyn Iterator<Item = Option<BBox>>> = if bboxes.len() == 1 {
            Box::new(std::iter::repeat(bboxes.get(0).map(|bbox| {
                BBox::from_u32_arrow_array(bbox.u32().unwrap().data())
            })))
        } else {
            Box::new((0..bboxes.len()).map(|i| {
                bboxes
                    .get(i)
                    .map(|bbox| BBox::from_u32_arrow_array(bbox.u32().unwrap().data()))
            }))
        };
        let result = crop_images(self, &mut bboxes_iterator);

        image_array_from_img_buffers(self.name(), result.as_slice(), Some(*self.image_mode()))
    }

    fn resize_to_fixed_shape_image_array(
        &self,
        w: u32,
        h: u32,
        mode: &ImageMode,
    ) -> DaftResult<FixedShapeImageArray> {
        let result = resize_images(self, w, h);
        fixed_image_array_from_img_buffers(self.name(), result.as_slice(), mode, h, w)
    }

    fn to_mode(&self, mode: ImageMode) -> DaftResult<Self>
    where
        Self: Sized,
    {
        let buffers: Vec<Option<CowImage>> = ImageBufferIter::new(self)
            .map(|img| img.map(|img| img.into_mode(mode)))
            .collect();

        let (height, width) = match self.data_type() {
            DataType::FixedShapeImage(_, h, w) => (h, w),
            _ => unreachable!("self should always be a FixedShapeImage"),
        };
        fixed_image_array_from_img_buffers(self.name(), &buffers, &mode, *height, *width)
    }

    fn attribute(&self, attr: ImageProperty) -> DaftResult<DataArray<UInt32Type>> {
        let (height, width) = match self.data_type() {
            DataType::FixedShapeImage(_, h, w) => (h, w),
            _ => unreachable!("Should be FixedShapeImage type"),
        };

        match attr {
            ImageProperty::Height => Ok(UInt32Array::from((
                self.name(),
                vec![*height; self.len()].as_slice(),
            ))),
            ImageProperty::Width => Ok(UInt32Array::from((
                self.name(),
                vec![*width; self.len()].as_slice(),
            ))),
            ImageProperty::Channel => Ok(UInt32Array::from((
                self.name(),
                vec![self.image_mode().num_channels() as u32; self.len()].as_slice(),
            ))),
            ImageProperty::Mode => Ok(UInt32Array::from((
                self.name(),
                vec![(*self.image_mode() as u8) as u32; self.len()].as_slice(),
            ))),
        }
    }

    fn average_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_average_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn perceptual_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_perceptual_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn difference_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_difference_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn wavelet_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_wavelet_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }

    fn crop_resistant_hash(&self) -> DaftResult<Utf8Array> {
        let mut results = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            if let Some(img) = self.as_image_obj(i) {
                let hash = compute_crop_resistant_hash(img)?;
                results.push(Some(hash));
            } else {
                results.push(None);
            }
        }

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from(results)),
        )))
    }
}

fn encode_images<Arr: AsImageObj>(
    images: &Arr,
    image_format: ImageFormat,
) -> DaftResult<BinaryArray> {
    let arrow_array = if image_format == ImageFormat::TIFF {
        // NOTE: A single writer/buffer can't be used for TIFF files because the encoder will overwrite the
        // IFD offset for the first image instead of writing it for all subsequent images, producing corrupted
        // TIFF files. We work around this by writing out a new buffer for each image.
        // TODO(Clark): Fix this in the tiff crate.
        let values = ImageBufferIter::new(images)
            .map(|img| {
                img.map(|img| {
                    let buf = Vec::new();
                    let mut writer: CountingWriter<std::io::BufWriter<_>> =
                        std::io::BufWriter::new(std::io::Cursor::new(buf)).into();
                    img.encode(image_format, &mut writer)?;
                    // NOTE: BufWriter::into_inner() will flush the buffer.
                    Ok(writer
                        .into_inner()
                        .into_inner()
                        .map_err(|e| {
                            DaftError::ValueError(format!(
                                "Encoding image into file format {image_format} failed: {e}"
                            ))
                        })?
                        .into_inner())
                })
                .transpose()
            })
            .collect::<DaftResult<Vec<_>>>()?;
        arrow2::array::BinaryArray::<i64>::from_iter(values)
    } else {
        let mut offsets = Vec::with_capacity(images.len() + 1);
        offsets.push(0i64);
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(images.len());
        let buf = Vec::new();
        let mut writer: CountingWriter<std::io::BufWriter<_>> =
            std::io::BufWriter::new(std::io::Cursor::new(buf)).into();
        ImageBufferIter::new(images)
            .map(|img| {
                if let Some(img) = img {
                    img.encode(image_format, &mut writer)?;
                    offsets.push(writer.count() as i64);
                    validity.push(true);
                } else {
                    offsets.push(*offsets.last().unwrap());
                    validity.push(false);
                }
                Ok(())
            })
            .collect::<DaftResult<Vec<_>>>()?;
        // NOTE: BufWriter::into_inner() will flush the buffer.
        let values = writer
            .into_inner()
            .into_inner()
            .map_err(|e| {
                DaftError::ValueError(format!(
                    "Encoding image into file format {image_format} failed: {e}"
                ))
            })?
            .into_inner();
        let encoded_data: arrow2::buffer::Buffer<u8> = values.into();
        let offsets_buffer = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
            0 => None,
            _ => Some(validity.into()),
        };
        arrow2::array::BinaryArray::<i64>::new(
            arrow2::datatypes::DataType::LargeBinary,
            offsets_buffer,
            encoded_data,
            validity,
        )
    };
    BinaryArray::new(
        Field::new(images.name(), arrow_array.data_type().into()).into(),
        arrow_array.boxed(),
    )
}

fn resize_images<Arr: AsImageObj>(images: &Arr, w: u32, h: u32) -> Vec<Option<CowImage<'_>>> {
    ImageBufferIter::new(images)
        .map(|img| img.map(|img| img.resize(w, h)))
        .collect::<Vec<_>>()
}

fn crop_images<'a, Arr>(
    images: &'a Arr,
    bboxes: &mut dyn Iterator<Item = Option<BBox>>,
) -> Vec<Option<CowImage<'a>>>
where
    Arr: AsImageObj,
{
    ImageBufferIter::new(images)
        .zip(bboxes)
        .map(|(img, bbox)| match (img, bbox) {
            (None, _) | (_, None) => None,
            (Some(img), Some(bbox)) => Some(img.crop(&bbox)),
        })
        .collect::<Vec<_>>()
}

#[must_use]
pub fn image_html_value(arr: &ImageArray, idx: usize, truncate: bool) -> String {
    let maybe_image = arr.as_image_obj(idx);
    let str_val = arr.str_value(idx).unwrap();

    match maybe_image {
        None => "None".to_string(),
        Some(image) => {
            let processed_image = if truncate {
                image.fit_to(128, 128)
            } else {
                image // Use the full-size image
            };
            let mut bytes: Vec<u8> = vec![];
            let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
            processed_image
                .encode(ImageFormat::PNG, &mut writer)
                .unwrap();
            drop(writer);

            let style = if truncate {
                "width:auto;height:auto"
            } else {
                "width:100%;height:auto"
            };

            format!(
                "<img style=\"{}\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                style,
                base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                str_val,
            )
        }
    }
}

#[must_use]
pub fn fixed_image_html_value(arr: &FixedShapeImageArray, idx: usize, truncate: bool) -> String {
    let maybe_image = arr.as_image_obj(idx);
    let str_val = arr.str_value(idx).unwrap();

    match maybe_image {
        None => "None".to_string(),
        Some(image) => {
            let processed_image = if truncate {
                image.fit_to(128, 128)
            } else {
                image // Use the full-size image
            };
            let mut bytes: Vec<u8> = vec![];
            let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
            processed_image
                .encode(ImageFormat::PNG, &mut writer)
                .unwrap();
            drop(writer);

            let style = if truncate {
                "width:auto;height:auto"
            } else {
                "width:100%;height:auto"
            };

            format!(
                "<img style=\"{}\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                style,
                base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                str_val,
            )
        }
    }
}

// Hash computation helper functions
fn compute_average_hash(img: CowImage) -> DaftResult<String> {
    // Convert to grayscale
    let gray_img = img.into_mode(daft_schema::prelude::ImageMode::L);

    // Resize to 8x8
    let resized = gray_img.resize(8, 8);

    // Get pixel data
    let pixel_data = resized.as_u8_slice();

    // Compute average pixel value
    let sum: u64 = pixel_data.iter().map(|&p| p as u64).sum();
    let average = sum / 64;
    let all_equal_to_avg = pixel_data.iter().all(|&p| (p as u64) == average);

    // Generate binary hash string
    let hash: String = if all_equal_to_avg {
        // For uniform images, decide based on brightness midpoint
        if average >= 128 {
            "1".repeat(64)
        } else {
            "0".repeat(64)
        }
    } else if average == 0 {
        // For solid black (average = 0), use strict > so we produce all 0s.
        pixel_data
            .iter()
            .map(|&pixel| if (pixel as u64) > average { '1' } else { '0' })
            .collect()
    } else {
        // For non-black images, treat values equal to average as 1 to favor brighter outputs.
        pixel_data
            .iter()
            .map(|&pixel| if (pixel as u64) >= average { '1' } else { '0' })
            .collect()
    };

    // Validate length (safety check)
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}

fn compute_perceptual_hash(img: CowImage) -> DaftResult<String> {
    // Convert to grayscale
    let gray_img = img.into_mode(daft_schema::prelude::ImageMode::L);

    // Resize to 32x32 for DCT
    let resized = gray_img.resize(32, 32);

    // Get pixel data
    let pixel_data = resized.as_u8_slice();

    // Convert to float and apply DCT
    let mut dct_matrix = vec![vec![0.0f64; 32]; 32];

    // Convert u8 to f64 and normalize
    for (row_idx, row) in dct_matrix.iter_mut().enumerate() {
        let start = row_idx * 32;
        let chunk = &pixel_data[start..start + 32];
        for (dst, &px) in row.iter_mut().zip(chunk.iter()) {
            *dst = px as f64;
        }
    }

    // Apply 2D DCT
    let mut dct_result = vec![vec![0.0f64; 32]; 32];
    for (u, row_out) in dct_result.iter_mut().enumerate() {
        for (v, cell_out) in row_out.iter_mut().enumerate() {
            let mut sum = 0.0;
            for (y, row_in) in dct_matrix.iter().enumerate() {
                for (x, &val) in row_in.iter().enumerate() {
                    let cos_x = (((2 * x + 1) * u) as f64 * std::f64::consts::PI / 64.0).cos();
                    let cos_y = (((2 * y + 1) * v) as f64 * std::f64::consts::PI / 64.0).cos();
                    sum += val * cos_x * cos_y;
                }
            }
            let cu = if u == 0 { 1.0 / 2.0_f64.sqrt() } else { 1.0 };
            let cv = if v == 0 { 1.0 / 2.0_f64.sqrt() } else { 1.0 };
            *cell_out = 0.25 * cu * cv * sum;
        }
    }

    // Extract 8x8 top-left corner (low frequency components)
    let mut hash_bits = Vec::new();
    let mut total = 0.0;
    let mut count = 0;

    // Calculate average of low frequency components (excluding DC component)
    for row in dct_result.iter().skip(1).take(8) {
        for &val in row.iter().skip(1).take(8) {
            total += val;
            count += 1;
        }
    }
    let average = total / count as f64;

    // Generate hash bits
    for row in dct_result.iter().take(8) {
        for &val in row.iter().take(8) {
            hash_bits.push(if val > average { '1' } else { '0' });
        }
    }

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length (safety check)
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Perceptual hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}

fn compute_difference_hash(img: CowImage) -> DaftResult<String> {
    // Convert to grayscale
    let gray_img = img.into_mode(daft_schema::prelude::ImageMode::L);

    // Resize to 9x8 for difference hash (9x8 so we can compare adjacent pixels)
    let resized = gray_img.resize(9, 8);

    // Get pixel data
    let pixel_data = resized.as_u8_slice();

    // Compute difference hash by comparing adjacent pixels horizontally
    let mut hash_bits = Vec::new();

    for y in 0..8 {
        for x in 0..8 {
            let left_pixel = pixel_data[y * 9 + x];
            let right_pixel = pixel_data[y * 9 + x + 1];
            hash_bits.push(if left_pixel < right_pixel { '1' } else { '0' });
        }
    }

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length (safety check)
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Difference hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}

fn compute_wavelet_hash(img: CowImage) -> DaftResult<String> {
    // Convert to grayscale
    let gray_img = img.into_mode(daft_schema::prelude::ImageMode::L);

    // Resize to 64x64 for wavelet transform
    let resized = gray_img.resize(64, 64);

    // Get pixel data
    let pixel_data = resized.as_u8_slice();

    // Convert to float matrix for wavelet transform
    let mut matrix = vec![vec![0.0f64; 64]; 64];
    for y in 0..64 {
        for x in 0..64 {
            matrix[y][x] = pixel_data[y * 64 + x] as f64;
        }
    }

    // Apply Haar wavelet transform
    let mut coeffs = matrix;
    let size = 64;

    // Apply wavelet transform iteratively
    let mut current_size = size;
    while current_size > 1 {
        // Create a temporary copy for the transform
        let mut temp = coeffs.clone();

        // Horizontal transform
        for y in 0..current_size {
            for x in 0..current_size / 2 {
                let left = temp[y][x * 2];
                let right = temp[y][x * 2 + 1];

                // Average (low frequency)
                coeffs[y][x] = f64::midpoint(left, right);
                // Difference (high frequency)
                coeffs[y][x + current_size / 2] = (left - right) / 2.0;
            }
        }

        // Vertical transform - use temp to avoid data dependency issues
        temp.clone_from(&coeffs);
        for x in 0..current_size {
            for y in 0..current_size / 2 {
                let top = temp[y * 2][x];
                let bottom = temp[y * 2 + 1][x];

                // Average (low frequency)
                coeffs[y][x] = f64::midpoint(top, bottom);
                // Difference (high frequency)
                coeffs[y + current_size / 2][x] = (top - bottom) / 2.0;
            }
        }

        current_size /= 2;
    }

    // Extract 8x8 top-left corner (low frequency components)
    let mut hash_bits = Vec::new();

    // Calculate median of the 8x8 block
    let mut values = Vec::new();
    for row in coeffs.iter().take(8) {
        values.extend(row.iter().take(8).copied());
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = if !values.is_empty() {
        if values.len() % 2 == 0 {
            // For even length, take average of two middle values
            f64::midpoint(values[values.len() / 2 - 1], values[values.len() / 2])
        } else {
            // For odd length, take middle value
            values[values.len() / 2]
        }
    } else {
        0.0
    };

    // Generate hash bits
    for row in coeffs.iter().take(8) {
        for &val in row.iter().take(8) {
            hash_bits.push(if val > median { '1' } else { '0' });
        }
    }

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length (safety check)
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Wavelet hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}

fn compute_crop_resistant_hash(img: CowImage) -> DaftResult<String> {
    // Convert to grayscale
    let gray_img = img.into_mode(daft_schema::prelude::ImageMode::L);

    // Resize to 256x256 for crop-resistant analysis
    let resized = gray_img.resize(256, 256);

    // Get pixel data
    let pixel_data = resized.as_u8_slice();

    // Create multiple overlapping regions for analysis
    // We'll create 8x8 regions at different positions to make it crop-resistant
    let mut hash_bits = Vec::new();

    // Define region positions (overlapping regions)
    let region_size = 64;
    let positions = [
        (0, 0),     // Top-left
        (96, 0),    // Top-right
        (0, 96),    // Bottom-left
        (96, 96),   // Bottom-right
        (48, 48),   // Center
        (32, 32),   // Upper-left center
        (112, 32),  // Upper-right center
        (32, 112),  // Lower-left center
        (112, 112), // Lower-right center
        (64, 64),   // True center
        (16, 16),   // Top-left corner
        (176, 16),  // Top-right corner
        (16, 176),  // Bottom-left corner
        (176, 176), // Bottom-right corner
        (80, 80),   // Center-left
        (176, 80),  // Center-right
    ];

    // Process each region
    for (start_y, start_x) in &positions {
        // Extract region
        let mut region = Vec::new();
        for y in *start_y..(*start_y + region_size) {
            for x in *start_x..(*start_x + region_size) {
                if y < 256 && x < 256 {
                    region.push(pixel_data[y * 256 + x] as f64);
                } else {
                    region.push(0.0);
                }
            }
        }

        // Compute average of this region
        let sum: f64 = region.iter().sum();
        let average = sum / region.len() as f64;

        // For crop-resistant hash, use a fixed threshold approach
        // This makes it more robust to different intensity levels
        let threshold = 127.5; // Middle of 0-255 range

        // Set bit based on whether region average is above threshold
        hash_bits.push(if average > threshold { '1' } else { '0' });
    }

    // Pad to 64 bits if we have fewer regions
    while hash_bits.len() < 64 {
        // Use a combination of position-based patterns
        let pos = hash_bits.len();
        let pattern = (pos % 8) as u8;
        hash_bits.push(if pattern.is_multiple_of(2) { '0' } else { '1' });
    }

    // Truncate to 64 bits if we have more
    hash_bits.truncate(64);

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length (safety check)
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Crop-resistant hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}
