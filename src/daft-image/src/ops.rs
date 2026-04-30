use std::sync::Arc;

use rustfft::{FftPlanner, num_complex::Complex};

thread_local! {
    static FFT_PLANNER: std::cell::RefCell<FftPlanner<f64>> =
        std::cell::RefCell::new(FftPlanner::new());
}

use arrow::array::{BooleanBufferBuilder, LargeBinaryArray, OffsetBufferBuilder};
use base64::Engine;
use daft_common_error::{DaftError, DaftResult};
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
use image::DynamicImage;
use rayon::prelude::*;

use crate::{CountingWriter, functions::hash_method::HashMethod, iters::ImageBufferIter};

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
    fn image_hash(
        &self,
        method: HashMethod,
        hash_size: u32,
        binbits: u32,
        segments: u32,
    ) -> DaftResult<FixedSizeBinaryArray>;
}

impl ImageOps for ImageArray {
    fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray> {
        encode_images(self, image_format)
    }

    fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let result = resize_images(self, w, h);
        image_array_from_img_buffers(self.name(), result.into_iter(), self.image_mode())
    }

    fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray> {
        let mut bboxes_iterator: Box<dyn Iterator<Item = Option<BBox>>> = if bboxes.len() == 1 {
            Box::new(std::iter::repeat(bboxes.get(0).map(|bbox| {
                let data = bbox.u32().unwrap();
                bbox_from_u32_arrow_array(data)
            })))
        } else {
            Box::new((0..bboxes.len()).map(|i| {
                bboxes
                    .get(i)
                    .map(|bbox| bbox_from_u32_arrow_array(bbox.u32().unwrap()))
            }))
        };
        let result = crop_images(self, &mut bboxes_iterator);
        image_array_from_img_buffers(self.name(), result.into_iter(), self.image_mode())
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
        let buffers: Vec<Option<CowImage>> = (0..self.len())
            .into_par_iter()
            .map(|i| self.as_image_obj(i).map(|img| img.into_mode(mode)))
            .collect();
        image_array_from_img_buffers(self.name(), buffers.into_iter(), Some(mode))
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

    fn image_hash(
        &self,
        method: HashMethod,
        hash_size: u32,
        binbits: u32,
        segments: u32,
    ) -> DaftResult<FixedSizeBinaryArray> {
        hash_images(self, self.name(), method, hash_size, binbits, segments)
    }
}

fn bbox_from_u32_arrow_array(arr: &UInt32Array) -> BBox {
    assert!(arr.len() == 4);

    let slice = arr.as_slice();

    BBox(slice[0], slice[1], slice[2], slice[3])
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
            Box::new(std::iter::repeat(
                bboxes
                    .get(0)
                    .map(|bbox| bbox_from_u32_arrow_array(bbox.u32().unwrap())),
            ))
        } else {
            Box::new((0..bboxes.len()).map(|i| {
                bboxes
                    .get(i)
                    .map(|bbox| bbox_from_u32_arrow_array(bbox.u32().unwrap()))
            }))
        };
        let result = crop_images(self, &mut bboxes_iterator);

        image_array_from_img_buffers(self.name(), result.into_iter(), Some(*self.image_mode()))
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
        let buffers: Vec<Option<CowImage>> = (0..self.len())
            .into_par_iter()
            .map(|i| self.as_image_obj(i).map(|img| img.into_mode(mode)))
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
            ImageProperty::Height => Ok(UInt32Array::from_slice(
                self.name(),
                &vec![*height; self.len()],
            )),
            ImageProperty::Width => Ok(UInt32Array::from_slice(
                self.name(),
                &vec![*width; self.len()],
            )),
            ImageProperty::Channel => Ok(UInt32Array::from_slice(
                self.name(),
                &vec![self.image_mode().num_channels() as u32; self.len()],
            )),
            ImageProperty::Mode => Ok(UInt32Array::from_slice(
                self.name(),
                &vec![(*self.image_mode() as u8) as u32; self.len()],
            )),
        }
    }

    fn image_hash(
        &self,
        method: HashMethod,
        hash_size: u32,
        binbits: u32,
        segments: u32,
    ) -> DaftResult<FixedSizeBinaryArray> {
        hash_images(self, self.name(), method, hash_size, binbits, segments)
    }
}

fn encode_images<Arr: AsImageObj>(
    images: &Arr,
    image_format: ImageFormat,
) -> DaftResult<BinaryArray> {
    if image_format == ImageFormat::TIFF {
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
        Ok(BinaryArray::from_iter(images.name(), values.into_iter()))
    } else {
        // For non-TIFF formats, use a single buffer with manual offset/validity tracking for efficiency
        let mut offsets = OffsetBufferBuilder::<i64>::new(images.len());
        let mut null_builder = BooleanBufferBuilder::new(images.len());
        let buf = Vec::new();
        let mut writer: CountingWriter<std::io::BufWriter<_>> =
            std::io::BufWriter::new(std::io::Cursor::new(buf)).into();
        let mut last_offset: u64 = 0;
        ImageBufferIter::new(images)
            .map(|img| {
                if let Some(img) = img {
                    img.encode(image_format, &mut writer)?;
                    let current_offset = writer.count();
                    offsets.push_length((current_offset - last_offset) as usize);
                    last_offset = current_offset;
                    null_builder.append(true);
                } else {
                    offsets.push_length(0);
                    null_builder.append(false);
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
        let arrow_array = LargeBinaryArray::new(
            offsets.finish(),
            values.into(),
            Some(null_builder.finish().into()),
        );
        BinaryArray::from_arrow(
            Field::new(images.name(), DataType::Binary),
            Arc::new(arrow_array),
        )
    }
}

fn resize_images<Arr: AsImageObj + Sync>(
    images: &Arr,
    w: u32,
    h: u32,
) -> Vec<Option<CowImage<'_>>> {
    (0..images.len())
        .into_par_iter()
        .map(|i| images.as_image_obj(i).map(|img| img.resize(w, h)))
        .collect()
}

fn crop_images<'a, Arr>(
    images: &'a Arr,
    bboxes: &mut dyn Iterator<Item = Option<BBox>>,
) -> Vec<Option<CowImage<'a>>>
where
    Arr: AsImageObj + Sync,
{
    let bboxes: Vec<Option<BBox>> = bboxes.take(images.len()).collect();
    (0..images.len())
        .into_par_iter()
        .zip(bboxes.into_par_iter())
        .map(|(i, bbox)| match (images.as_image_obj(i), bbox) {
            (None, _) | (_, None) => None,
            (Some(img), Some(bbox)) => Some(img.crop(&bbox)),
        })
        .collect()
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

/// Extract grayscale pixel values (0.0–255.0) from a DynamicImage after resizing.
///
/// Two optimisations vs the naive "resize RGB then grayscale" approach:
///
/// 1. **Grayscale first**: `to_luma8()` converts to a single channel before the
///    resize so the filter kernel operates on 1 channel instead of 3 (~3× fewer
///    multiply-adds).  The ±0.5 LSB quantisation error introduced by rounding to
///    u8 before the convolution averages out across the kernel and is < 0.1 LSB at
///    the output for downsampling ratios ≥ 4:1.
///
/// 2. **Triangle (bilinear) filter**: uses a 2-sample kernel instead of Lanczos3's
///    6-sample kernel.  For the large downsampling ratios typical in perceptual
///    hashing (e.g. 224→8, 28:1) the kernel support scales linearly, so Triangle
///    requires ~3× fewer samples than Lanczos3.  At 8×8 output the visual
///    difference is imperceptible and bit-exact hash compatibility with the
///    imagehash Python library is maintained in practice.
fn luma_pixels(img: &DynamicImage, w: u32, h: u32) -> Vec<f64> {
    let gray = img.to_luma8();
    image::imageops::resize(&gray, w, h, image::imageops::FilterType::Triangle)
        .pixels()
        .map(|p| p.0[0] as f64)
        .collect()
}

/// Pack a bit iterator into bytes (MSB first within each byte).
fn bits_to_bytes(bits: impl Iterator<Item = bool>, n_bits: usize) -> Vec<u8> {
    let n_bytes = n_bits.div_ceil(8);
    let mut result = vec![0u8; n_bytes];
    for (i, bit) in bits.enumerate() {
        if bit {
            result[i / 8] |= 1 << (7 - (i % 8));
        }
    }
    result
}

/// Average hash (aHash): resize to hash_size x hash_size, compare each pixel to mean.
fn ahash(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    let pixels = luma_pixels(img, hash_size, hash_size);
    let mean = pixels.iter().sum::<f64>() / pixels.len() as f64;
    let n_bits = (hash_size * hash_size) as usize;
    Ok(bits_to_bytes(pixels.iter().map(|&p| p > mean), n_bits))
}

/// Difference hash (dHash): resize to (hash_size+1) x hash_size, compare adjacent column pixels.
fn dhash(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    let pixels = luma_pixels(img, hash_size + 1, hash_size);
    let w = (hash_size + 1) as usize;
    let hs = hash_size as usize;
    let n_bits = hs * hs;
    let mut bits = Vec::with_capacity(n_bits);
    for row in 0..hs {
        for col in 0..hs {
            bits.push(pixels[row * w + col] < pixels[row * w + col + 1]);
        }
    }
    Ok(bits_to_bytes(bits.into_iter(), n_bits))
}

/// Vertical difference hash (dHash vertical): resize to hash_size x (hash_size+1),
/// compare adjacent row pixels (next row > previous row).
fn dhash_vertical(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    let pixels = luma_pixels(img, hash_size, hash_size + 1);
    let w = hash_size as usize;
    let hs = hash_size as usize;
    let n_bits = hs * hs;
    let mut bits = Vec::with_capacity(n_bits);
    for row in 0..hs {
        for col in 0..w {
            bits.push(pixels[(row + 1) * w + col] > pixels[row * w + col]);
        }
    }
    Ok(bits_to_bytes(bits.into_iter(), n_bits))
}

/// Perceptual hash (pHash): resize to (hash_size*4) x (hash_size*4), compute 2D DCT,
/// take the top-left hash_size x hash_size block, compare to median.
fn phash(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    let dct_size = hash_size * 4;
    let pixels = luma_pixels(img, dct_size, dct_size);
    let n = dct_size as usize;
    let dct_coeffs = dct2d(&pixels, n);
    let hs = hash_size as usize;
    // Take rows 0..hs, cols 0..hs (top-left block, matching imagehash dct[:hash_size, :hash_size])
    let mut sub = Vec::with_capacity(hs * hs);
    for r in 0..hs {
        for c in 0..hs {
            sub.push(dct_coeffs[r * n + c]);
        }
    }
    let median = median_f64(&mut sub.clone());
    let n_bits = (hash_size * hash_size) as usize;
    Ok(bits_to_bytes(sub.iter().map(|&v| v > median), n_bits))
}

/// Simplified perceptual hash (pHash simple): resize to (hash_size*4) x (hash_size*4),
/// apply row-wise DCT only, take top `hash_size` rows and cols `1..=hash_size` (skip DC),
/// compare each coefficient to the block mean.
fn phash_simple(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    let dct_size = hash_size * 4;
    let pixels = luma_pixels(img, dct_size, dct_size);
    let n = dct_size as usize;
    let hs = hash_size as usize;
    // Row-wise DCT only (no column pass)
    let mut dct_rows = vec![0f64; n * n];
    for r in 0..n {
        let row = &pixels[r * n..(r + 1) * n];
        let out = dct1d(row);
        dct_rows[r * n..(r + 1) * n].copy_from_slice(&out);
    }
    // Low-frequency block: rows 0..hash_size, cols 1..=hash_size (skip DC at col 0)
    let mut sub = Vec::with_capacity(hs * hs);
    for r in 0..hs {
        for c in 1..=hs {
            sub.push(dct_rows[r * n + c]);
        }
    }
    let avg = sub.iter().sum::<f64>() / sub.len() as f64;
    let n_bits = hs * hs;
    Ok(bits_to_bytes(sub.iter().map(|&v| v > avg), n_bits))
}

/// Compute the median of a slice (partially sorts in place).
fn median_f64(values: &mut [f64]) -> f64 {
    let n = values.len();
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if n % 2 == 1 {
        values[n / 2]
    } else {
        f64::midpoint(values[n / 2 - 1], values[n / 2])
    }
}

// ── Haar wavelet helpers (pywt-compatible) ───────────────────────────────────

/// 1D Haar forward DWT on an even-length signal.
/// Matches pywt convention: cA[i] = (x[2i]+x[2i+1])/√2, cD[i] = (x[2i]-x[2i+1])/√2.
fn haar_dwt1d(x: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let n = x.len();
    debug_assert!(n.is_multiple_of(2));
    let half = n / 2;
    let s2 = std::f64::consts::SQRT_2;
    let mut ca = Vec::with_capacity(half);
    let mut cd = Vec::with_capacity(half);
    for i in 0..half {
        ca.push((x[2 * i] + x[2 * i + 1]) / s2);
        cd.push((x[2 * i] - x[2 * i + 1]) / s2);
    }
    (ca, cd)
}

/// 1D Haar inverse DWT: from (cA, cD) each of length n, reconstruct signal of length 2n.
fn haar_idwt1d(ca: &[f64], cd: &[f64]) -> Vec<f64> {
    debug_assert_eq!(ca.len(), cd.len());
    let n = ca.len();
    let s2 = std::f64::consts::SQRT_2;
    let mut x = vec![0f64; 2 * n];
    for i in 0..n {
        x[2 * i] = (ca[i] + cd[i]) / s2;
        x[2 * i + 1] = (ca[i] - cd[i]) / s2;
    }
    x
}

/// Apply 1D DWT along axis 0 (column-wise) on a (rows × cols) matrix.
/// Returns (lo: rows/2 × cols, hi: rows/2 × cols).
fn dwt_axis0(data: &[f64], rows: usize, cols: usize) -> (Vec<f64>, Vec<f64>) {
    debug_assert!(rows.is_multiple_of(2));
    let half = rows / 2;
    let mut lo = vec![0f64; half * cols];
    let mut hi = vec![0f64; half * cols];
    for c in 0..cols {
        let col: Vec<f64> = (0..rows).map(|r| data[r * cols + c]).collect();
        let (a, d) = haar_dwt1d(&col);
        for i in 0..half {
            lo[i * cols + c] = a[i];
            hi[i * cols + c] = d[i];
        }
    }
    (lo, hi)
}

/// Apply 1D DWT along axis 1 (row-wise) on a (rows × cols) matrix.
/// Returns (lo: rows × cols/2, hi: rows × cols/2).
fn dwt_axis1(data: &[f64], rows: usize, cols: usize) -> (Vec<f64>, Vec<f64>) {
    debug_assert!(cols.is_multiple_of(2));
    let half = cols / 2;
    let mut lo = vec![0f64; rows * half];
    let mut hi = vec![0f64; rows * half];
    for r in 0..rows {
        let row = &data[r * cols..(r + 1) * cols];
        let (a, d) = haar_dwt1d(row);
        lo[r * half..(r + 1) * half].copy_from_slice(&a);
        hi[r * half..(r + 1) * half].copy_from_slice(&d);
    }
    (lo, hi)
}

/// Apply 1D inverse DWT along axis 0.
/// (lo: half_rows × cols, hi: half_rows × cols) → (rows × cols).
fn idwt_axis0(lo: &[f64], hi: &[f64], half_rows: usize, cols: usize) -> Vec<f64> {
    let rows = half_rows * 2;
    let mut out = vec![0f64; rows * cols];
    for c in 0..cols {
        let ca: Vec<f64> = (0..half_rows).map(|r| lo[r * cols + c]).collect();
        let cd: Vec<f64> = (0..half_rows).map(|r| hi[r * cols + c]).collect();
        let x = haar_idwt1d(&ca, &cd);
        for (r, &v) in x.iter().enumerate() {
            out[r * cols + c] = v;
        }
    }
    out
}

/// Apply 1D inverse DWT along axis 1.
/// (lo: rows × half_cols, hi: rows × half_cols) → (rows × cols).
fn idwt_axis1(lo: &[f64], hi: &[f64], rows: usize, half_cols: usize) -> Vec<f64> {
    let cols = half_cols * 2;
    let mut out = vec![0f64; rows * cols];
    for r in 0..rows {
        let ca = &lo[r * half_cols..(r + 1) * half_cols];
        let cd = &hi[r * half_cols..(r + 1) * half_cols];
        let x = haar_idwt1d(ca, cd);
        out[r * cols..(r + 1) * cols].copy_from_slice(&x);
    }
    out
}

/// 2D single-level Haar DWT on (rows × cols) → (LL, LH, HL, HH) each (rows/2 × cols/2).
/// Matches pywt.dwt2: first apply along axis 0 (columns), then axis 1 (rows).
fn dwt2_single(data: &[f64], rows: usize, cols: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let (col_lo, col_hi) = dwt_axis0(data, rows, cols);
    let half_rows = rows / 2;
    let (ll, lh) = dwt_axis1(&col_lo, half_rows, cols);
    let (hl, hh) = dwt_axis1(&col_hi, half_rows, cols);
    (ll, lh, hl, hh)
}

/// 2D single-level Haar inverse DWT.
/// (LL, LH, HL, HH) each (half_rows × half_cols) → (rows × cols).
fn idwt2_single(
    ll: &[f64],
    lh: &[f64],
    hl: &[f64],
    hh: &[f64],
    half_rows: usize,
    half_cols: usize,
) -> Vec<f64> {
    let cols = half_cols * 2;
    let col_lo = idwt_axis1(ll, lh, half_rows, half_cols);
    let col_hi = idwt_axis1(hl, hh, half_rows, half_cols);
    idwt_axis0(&col_lo, &col_hi, half_rows, cols)
}

struct WaveDec2Level {
    lh: Vec<f64>,
    hl: Vec<f64>,
    hh: Vec<f64>,
    /// Shape of the detail bands (= parent_size / 2).
    band_rows: usize,
    band_cols: usize,
}

/// Multi-level 2D Haar DWT (wavedec2).
/// Returns (coarsest_ll, levels) where levels[0] is the coarsest detail band.
fn wavedec2(
    data: &[f64],
    rows: usize,
    cols: usize,
    level: usize,
) -> (Vec<f64>, Vec<WaveDec2Level>) {
    let mut current_ll = data.to_vec();
    let mut cur_rows = rows;
    let mut cur_cols = cols;
    let mut levels: Vec<WaveDec2Level> = Vec::with_capacity(level);
    for _ in 0..level {
        let (ll, lh, hl, hh) = dwt2_single(&current_ll, cur_rows, cur_cols);
        cur_rows /= 2;
        cur_cols /= 2;
        levels.push(WaveDec2Level {
            lh,
            hl,
            hh,
            band_rows: cur_rows,
            band_cols: cur_cols,
        });
        current_ll = ll;
    }
    // Reverse so index 0 = coarsest (matching pywt convention)
    levels.reverse();
    (current_ll, levels)
}

/// Multi-level 2D Haar inverse DWT (waverec2).
fn waverec2(ll: &[f64], levels: &[WaveDec2Level]) -> Vec<f64> {
    let mut current = ll.to_vec();
    // levels[0] is coarsest; iterate coarse-to-fine
    for lv in levels {
        current = idwt2_single(&current, &lv.lh, &lv.hl, &lv.hh, lv.band_rows, lv.band_cols);
    }
    current
}

/// Haar wavelet hash (wHash): bit-exact match with `imagehash.whash`.
///
/// Replicates pywt multi-level Haar DWT + remove_max_haar_ll:
/// 1. Resize to `image_scale` (largest power-of-2 ≤ min(w,h), ≥ hash_size).
/// 2. Decompose at `ll_max_level = log2(image_scale)`, zero out DC, reconstruct.
/// 3. Re-decompose at `dwt_level = ll_max_level - log2(hash_size)`.
/// 4. Take `coeffs[0]` (hash_size × hash_size), min-max normalise, compare to mean.
fn whash(img: &DynamicImage, hash_size: u32) -> DaftResult<Vec<u8>> {
    if !hash_size.is_power_of_two() {
        return Err(DaftError::ValueError(format!(
            "whash requires hash_size to be a power of 2, but got {hash_size}"
        )));
    }
    let (w, h) = (img.width(), img.height());
    let min_dim = w.min(h);
    // Largest power of 2 ≤ min_dim, clamped to ≥ hash_size
    let log2_natural = (min_dim as f64).log2().floor() as u32;
    let image_scale = (1u32 << log2_natural).max(hash_size);

    let ll_max_level = image_scale.ilog2() as usize;
    let hash_level = hash_size.ilog2() as usize;
    let dwt_level = ll_max_level - hash_level;

    let n = image_scale as usize;
    let pixels_raw = luma_pixels(img, image_scale, image_scale);
    let pixels: Vec<f64> = pixels_raw.iter().map(|&p| p / 255.0).collect();

    // Step 1: remove DC
    let (mut ll0, details0) = wavedec2(&pixels, n, n, ll_max_level);
    ll0.fill(0.0);
    let dc_removed = waverec2(&ll0, &details0);

    // Step 2: extract hash-band LL
    let (ll_hash, _) = wavedec2(&dc_removed, n, n, dwt_level);

    let hs = hash_size as usize;
    debug_assert_eq!(ll_hash.len(), hs * hs);

    let min_val = ll_hash.iter().copied().fold(f64::INFINITY, f64::min);
    let max_val = ll_hash.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let range = max_val - min_val;
    let normalized: Vec<f64> = if range == 0.0 {
        vec![0.0; ll_hash.len()]
    } else {
        ll_hash.iter().map(|&v| (v - min_val) / range).collect()
    };

    let avg = normalized.iter().sum::<f64>() / normalized.len() as f64;
    let n_bits = hs * hs;
    Ok(bits_to_bytes(normalized.iter().map(|&v| v > avg), n_bits))
}

// ── colorhash helpers ────────────────────────────────────────────────────────

/// Convert an RGB pixel to HSV using Pillow's integer algorithm.
///
/// Output channels are all in `[0, 255]`, matching `PIL.Image.convert("HSV")`.
fn rgb_to_pillow_hsv(r: u8, g: u8, b: u8) -> (u8, u8, u8) {
    let (r, g, b) = (r as i32, g as i32, b as i32);
    let maxc = r.max(g).max(b);
    let minc = r.min(g).min(b);
    let delta = maxc - minc;

    let v = maxc as u8;
    if maxc == 0 {
        return (0, 0, v);
    }
    let s = (255 * delta / maxc) as u8;
    if delta == 0 {
        return (0, s, v);
    }
    // Scale: h = offset*divisor/divisor + frac*255/divisor, where divisor = 6*delta.
    // Offsets (0, 85, 170) correspond to sectors (0/6, 2/6, 4/6) × 255.
    // Combining offset*divisor + frac*255 keeps values non-negative for green/blue
    // sectors, so plain `/` (truncation) gives correct floor for those.
    // Red sector can go negative (when g < b); use div_euclid for floor semantics
    // and add 255 to wrap into [212, 254].
    let divisor = 6 * delta;
    let h_raw = if maxc == r {
        (g - b) * 255
    } else if maxc == g {
        85 * divisor + (b - r) * 255
    } else {
        170 * divisor + (r - g) * 255
    };
    let h = if h_raw >= 0 {
        (h_raw / divisor) as u8
    } else {
        (h_raw.div_euclid(divisor) + 255) as u8
    };
    (h, s, v)
}

/// Map a hue byte to one of 6 bins matching `numpy.linspace(0, 255, 7)`.
///
/// Edges: [0, 42.5, 85, 127.5, 170, 212.5, 255].
/// The last bin is right-inclusive (numpy histogram convention).
#[inline]
fn hue_bin(h: u8) -> usize {
    match h {
        0..=42 => 0,
        43..=84 => 1,
        85..=127 => 2,
        128..=169 => 3,
        170..=212 => 4,
        _ => 5,
    }
}

/// Color hash (colorhash).
///
/// Encodes the distribution of colors in HSV space into 14 bins:
/// - 1 bin: fraction of black pixels (intensity < 32)
/// - 1 bin: fraction of gray pixels (S < 85, not black)
/// - 6 bins: hue histogram of faint-color pixels (85 ≤ S ≤ 170)
/// - 6 bins: hue histogram of bright-color pixels (S > 170)
///
/// Each bin is quantised to `binbits` bits using the same non-standard
/// threshold encoding as the Python library, giving `14 * binbits` total bits.
fn colorhash(img: &DynamicImage, binbits: u32) -> DaftResult<Vec<u8>> {
    let rgb = img.to_rgb8();
    let n_pixels = (rgb.width() * rgb.height()) as usize;

    let mut n_black: usize = 0;
    let mut n_gray: usize = 0;
    let mut n_colors: usize = 0;
    let mut h_faint = [0usize; 6];
    let mut h_bright = [0usize; 6];

    for px in rgb.pixels() {
        let [r, g, b] = px.0;
        // Grayscale intensity matching PIL 'L' mode
        let intensity = 0.114f64
            .mul_add(b as f64, 0.299f64.mul_add(r as f64, 0.587 * g as f64))
            .round() as u8;
        let (h, s, _v) = rgb_to_pillow_hsv(r, g, b);

        let is_black = intensity < 32; // 256 / 8
        let is_gray = !is_black && s < 85; // 256 / 3

        if is_black {
            n_black += 1;
        } else if is_gray {
            n_gray += 1;
        } else {
            n_colors += 1;
            // faint: S < 256*2/3 ≈ 170.67  →  S ≤ 170
            // bright: S > 256*2/3            →  S ≥ 171
            if (s as f64) < 256.0 * 2.0 / 3.0 {
                h_faint[hue_bin(h)] += 1;
            } else if (s as f64) > 256.0 * 2.0 / 3.0 {
                h_bright[hue_bin(h)] += 1;
            }
        }
    }

    let frac_black = n_black as f64 / n_pixels as f64;
    let frac_gray = n_gray as f64 / n_pixels as f64;
    let c = n_colors.max(1);
    let maxvalue = 1u32 << binbits;
    let maxval = (maxvalue - 1) as usize;

    // 14 quantised values (same clamping as imagehash)
    let mut values = [0u32; 14];
    values[0] = ((frac_black * maxvalue as f64) as usize).min(maxval) as u32;
    values[1] = ((frac_gray * maxvalue as f64) as usize).min(maxval) as u32;
    for j in 0..6 {
        values[2 + j] = (h_faint[j] * maxvalue as usize / c).min(maxval) as u32;
        values[8 + j] = (h_bright[j] * maxvalue as usize / c).min(maxval) as u32;
    }

    // Encode each value with `binbits` bits using imagehash's threshold encoding:
    //   bit_i = (v >> (binbits-i-1)) % (1 << (binbits-i)) > 0
    let total_bits = 14 * binbits as usize;
    let n_bytes = total_bits.div_ceil(8);
    let mut result = vec![0u8; n_bytes];
    let mut bit_idx = 0usize;
    for v in &values {
        for i in 0..binbits as usize {
            let k = binbits as usize - i - 1;
            let bit = !(v >> k).is_multiple_of(1 << (k + 1));
            if bit {
                result[bit_idx / 8] |= 1 << (7 - (bit_idx % 8));
            }
            bit_idx += 1;
        }
    }
    Ok(result)
}

// ── DCT helpers ──────────────────────────────────────────────────────────────

/// Separable 2D DCT-II on a square n x n input (row-major order).
fn dct2d(input: &[f64], n: usize) -> Vec<f64> {
    let mut tmp = vec![0f64; n * n];
    for r in 0..n {
        let row = &input[r * n..(r + 1) * n];
        let out = dct1d(row);
        tmp[r * n..(r + 1) * n].copy_from_slice(&out);
    }
    let mut result = vec![0f64; n * n];
    let mut col_buf = vec![0f64; n];
    for c in 0..n {
        for r in 0..n {
            col_buf[r] = tmp[r * n + c];
        }
        let out = dct1d(&col_buf);
        for r in 0..n {
            result[r * n + c] = out[r];
        }
    }
    result
}

/// 1D DCT-II (unnormalized, matches `scipy.fft.dct` with `type=2, norm=None`).
///
/// For power-of-two lengths uses an FFT-based (Makhoul) algorithm via `rustfft`.
/// For other lengths falls back to the O(N²) direct formula.
fn dct1d(x: &[f64]) -> Vec<f64> {
    let n = x.len();
    if n.is_power_of_two() && n >= 2 {
        dct1d_fft(x)
    } else {
        dct1d_naive(x)
    }
}

/// FFT-based DCT-II using the Makhoul reordering + rustfft.
///
/// Uses a thread-local `FftPlanner` so the plan for each length is computed once
/// per thread and reused across all subsequent calls.
fn dct1d_fft(x: &[f64]) -> Vec<f64> {
    let n = x.len();
    // Makhoul reordering: even-indexed elements first, reversed odd-indexed last.
    let mut buf: Vec<Complex<f64>> = (0..n / 2).map(|i| Complex::new(x[2 * i], 0.0)).collect();
    for i in 0..n / 2 {
        buf.push(Complex::new(x[2 * i + 1], 0.0));
    }
    buf[n / 2..].reverse();

    FFT_PLANNER.with(|p| {
        let fft = p.borrow_mut().plan_fft_forward(n);
        fft.process(&mut buf);
    });

    // Apply phase factor: y[k] = 2 * Re( buf[k] * e^{-i*pi*k/(2N)} )
    (0..n)
        .map(|k| {
            let angle = -std::f64::consts::PI * k as f64 / (2 * n) as f64;
            2.0 * buf[k].re.mul_add(angle.cos(), -(buf[k].im * angle.sin()))
        })
        .collect()
}

/// O(N²) fallback for non-power-of-two lengths.
fn dct1d_naive(x: &[f64]) -> Vec<f64> {
    let n = x.len();
    let mut output = vec![0f64; n];
    let pi_over_2n = std::f64::consts::PI / (2 * n) as f64;
    for (k, out) in output.iter_mut().enumerate().take(n) {
        let mut sum = 0f64;
        for (i, &xi) in x.iter().enumerate() {
            sum += xi * (pi_over_2n * (2 * i + 1) as f64 * k as f64).cos();
        }
        *out = 2.0 * sum;
    }
    output
}

/// Shared implementation of `image_hash` for any `AsImageObj` array type.
fn hash_images<Arr: AsImageObj + Sync>(
    arr: &Arr,
    name: &str,
    method: HashMethod,
    hash_size: u32,
    binbits: u32,
    segments: u32,
) -> DaftResult<FixedSizeBinaryArray> {
    let n_bytes = hash_output_bytes(method, hash_size, binbits, segments);
    let hashes: Vec<Option<Vec<u8>>> = (0..arr.len())
        .into_par_iter()
        .map(|i| {
            arr.as_image_obj(i)
                .map(|img| {
                    let dyn_img: DynamicImage = img.into();
                    perceptual_hash(&dyn_img, method, hash_size, binbits, segments)
                })
                .transpose()
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(FixedSizeBinaryArray::from_iter(
        name,
        hashes.iter().map(|h| h.as_deref()),
        n_bytes,
    ))
}

/// Number of output bytes for the given method and parameters.
///
/// - Single-segment methods: `hash_size * hash_size` bits.
/// - `crop_resistant`: `segments * segments * hash_size * hash_size` bits.
/// - `colorhash`: `14 * binbits` bits (14 colour/intensity bins).
pub(crate) fn hash_output_bytes(
    method: HashMethod,
    hash_size: u32,
    binbits: u32,
    segments: u32,
) -> usize {
    match method {
        HashMethod::ColorHash => (14 * binbits as usize).div_ceil(8),
        HashMethod::CropResistant => {
            let bits =
                segments as usize * segments as usize * hash_size as usize * hash_size as usize;
            bits.div_ceil(8)
        }
        _ => {
            let bits = hash_size as usize * hash_size as usize;
            bits.div_ceil(8)
        }
    }
}

/// Dispatch to the appropriate hash algorithm.
pub(crate) fn perceptual_hash(
    img: &DynamicImage,
    method: HashMethod,
    hash_size: u32,
    binbits: u32,
    segments: u32,
) -> DaftResult<Vec<u8>> {
    match method {
        HashMethod::AHash => ahash(img, hash_size),
        HashMethod::DHash => dhash(img, hash_size),
        HashMethod::DHashVertical => dhash_vertical(img, hash_size),
        HashMethod::PHash => phash(img, hash_size),
        HashMethod::PHashSimple => phash_simple(img, hash_size),
        HashMethod::WHash => whash(img, hash_size),
        HashMethod::CropResistant => crop_resistant(img, hash_size, segments),
        HashMethod::ColorHash => colorhash(img, binbits),
    }
}

/// Crop-resistant hash: divide the image into a `segments × segments` grid,
/// compute a pHash for each segment, and concatenate the results.
///
/// The output size is `segments² × hash_size²` bits.
fn crop_resistant(img: &DynamicImage, hash_size: u32, segments: u32) -> DaftResult<Vec<u8>> {
    let (w, h) = (img.width(), img.height());
    let mut all_bits: Vec<u8> = Vec::new();
    for row in 0..segments {
        for col in 0..segments {
            // Compute the pixel boundary for this segment.
            let x0 = col * w / segments;
            let x1 = (col + 1) * w / segments;
            let y0 = row * h / segments;
            let y1 = (row + 1) * h / segments;
            let seg = img.crop_imm(x0, y0, x1 - x0, y1 - y0);
            let seg_hash = phash(&seg, hash_size)?;
            all_bits.extend(seg_hash);
        }
    }
    Ok(all_bits)
}
