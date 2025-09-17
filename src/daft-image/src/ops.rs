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

    // Generate binary hash string - simplified and consistent
    let hash: String = pixel_data
        .iter()
        .map(|&pixel| if (pixel as u64) >= average { '1' } else { '0' })
        .collect();

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

    // Convert to float matrix
    let mut dct_input = vec![vec![0.0f64; 32]; 32];
    for y in 0..32 {
        for x in 0..32 {
            dct_input[y][x] = pixel_data[y * 32 + x] as f64;
        }
    }

    // Apply 2D DCT (corrected implementation)
    let mut dct_result = vec![vec![0.0f64; 32]; 32];
    
    for u in 0..32 {
        for v in 0..32 {
            let mut sum = 0.0;
            
            for x in 0..32 {
                for y in 0..32 {
                    let cos_u = ((2.0 * x as f64 + 1.0) * u as f64 * std::f64::consts::PI / 64.0).cos();
                    let cos_v = ((2.0 * y as f64 + 1.0) * v as f64 * std::f64::consts::PI / 64.0).cos();
                    sum += dct_input[y][x] * cos_u * cos_v;
                }
            }
            
            let cu = if u == 0 { 1.0 / (2.0_f64).sqrt() } else { 1.0 };
            let cv = if v == 0 { 1.0 / (2.0_f64).sqrt() } else { 1.0 };
            dct_result[u][v] = 0.25 * cu * cv * sum;
        }
    }

    // Extract 8x8 top-left corner (excluding DC component at [0,0])
    let mut coefficients = Vec::new();
    for u in 0..8 {
        for v in 0..8 {
            if u != 0 || v != 0 { // Skip DC component
                coefficients.push(dct_result[u][v]);
            }
        }
    }

    // Calculate median of the low frequency components
    coefficients.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if coefficients.is_empty() {
        0.0
    } else {
        coefficients[coefficients.len() / 2]
    };

    // Generate hash bits (excluding DC component)
    let mut hash_bits = Vec::new();
    for u in 0..8 {
        for v in 0..8 {
            if u == 0 && v == 0 {
                continue; // Skip DC component
            }
            hash_bits.push(if dct_result[u][v] > median { '1' } else { '0' });
        }
    }

    // Pad to 64 bits if needed (should be 63 without DC, so pad with one bit)
    while hash_bits.len() < 64 {
        hash_bits.push('0');
    }
    hash_bits.truncate(64);

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length
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

    // Convert to float matrix
    let mut coeffs = vec![vec![0.0f64; 64]; 64];
    for y in 0..64 {
        for x in 0..64 {
            coeffs[y][x] = pixel_data[y * 64 + x] as f64;
        }
    }

    // Apply Haar wavelet transform (corrected implementation)
    let mut current_size = 64;
    
    while current_size > 1 {
        let half_size = current_size / 2;
        let mut temp = coeffs.clone();

        // Horizontal transform
        for y in 0..current_size {
            for x in 0..half_size {
                let left = temp[y][x * 2];
                let right = temp[y][x * 2 + 1];
                
                // Low frequency (average)
                coeffs[y][x] = (left + right) / 2.0;
                // High frequency (difference)
                coeffs[y][x + half_size] = (left - right) / 2.0;
            }
        }

        // Update temp for vertical transform
        temp = coeffs.clone();

        // Vertical transform
        for x in 0..current_size {
            for y in 0..half_size {
                let top = temp[y * 2][x];
                let bottom = temp[y * 2 + 1][x];
                
                // Low frequency (average)
                coeffs[y][x] = (top + bottom) / 2.0;
                // High frequency (difference)
                coeffs[y + half_size][x] = (top - bottom) / 2.0;
            }
        }

        current_size = half_size;
    }

    // Extract 8x8 top-left corner (low frequency components)
    let mut coefficients = Vec::new();
    for y in 0..8 {
        for x in 0..8 {
            coefficients.push(coeffs[y][x]);
        }
    }

    // Calculate median
    coefficients.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = coefficients[coefficients.len() / 2];

    // Generate hash bits
    let mut hash_bits = Vec::new();
    for y in 0..8 {
        for x in 0..8 {
            hash_bits.push(if coeffs[y][x] > median { '1' } else { '0' });
        }
    }

    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length
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

    // Resize to 256x256 for analysis
    let resized = gray_img.resize(256, 256);
    let pixel_data = resized.as_u8_slice();

    let mut hash_bits = Vec::new();

    // Use ring-based sampling from center for crop resistance
    let center_x = 128.0;
    let center_y = 128.0;
    
    // Sample at different radii and angles
    let radii = [30.0, 50.0, 70.0, 90.0]; // Different distances from center
    let angles_per_ring = 16; // 16 samples per ring
    
    for &radius in &radii {
        let mut ring_values = Vec::new();
        
        // Sample points around the ring
        for i in 0..angles_per_ring {
            let angle = 2.0 * std::f64::consts::PI * i as f64 / angles_per_ring as f64;
            let x = center_x + radius * angle.cos();
            let y = center_y + radius * angle.sin();
            
            // Bilinear interpolation for sub-pixel sampling
            let x_floor = x.floor() as usize;
            let y_floor = y.floor() as usize;
            let x_frac = x - x_floor as f64;
            let y_frac = y - y_floor as f64;
            
            if x_floor < 255 && y_floor < 255 {
                let top_left = pixel_data[y_floor * 256 + x_floor] as f64;
                let top_right = pixel_data[y_floor * 256 + (x_floor + 1)] as f64;
                let bottom_left = pixel_data[(y_floor + 1) * 256 + x_floor] as f64;
                let bottom_right = pixel_data[(y_floor + 1) * 256 + (x_floor + 1)] as f64;
                
                let top = top_left * (1.0 - x_frac) + top_right * x_frac;
                let bottom = bottom_left * (1.0 - x_frac) + bottom_right * x_frac;
                let interpolated = top * (1.0 - y_frac) + bottom * y_frac;
                
                ring_values.push(interpolated);
            } else {
                ring_values.push(0.0); // Outside bounds
            }
        }
        
        // Calculate median for this ring
        ring_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if !ring_values.is_empty() {
            ring_values[ring_values.len() / 2]
        } else {
            0.0
        };
        
        // Generate bits by comparing each sample to the ring's median
        for &value in &ring_values {
            hash_bits.push(if value > median { '1' } else { '0' });
        }
    }
    
    // We should have 64 bits (4 rings * 16 samples each)
    hash_bits.truncate(64);
    
    let hash = hash_bits.into_iter().collect::<String>();

    // Validate length
    if hash.len() != 64 {
        return Err(DaftError::ValueError(format!(
            "Crop-resistant hash should be 64 characters, got {}",
            hash.len()
        )));
    }

    Ok(hash)
}
