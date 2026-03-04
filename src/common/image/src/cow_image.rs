use std::{
    borrow::Cow,
    io::{Seek, Write},
    ops::Deref,
};

use common_error::{DaftError, DaftResult};
use daft_schema::prelude::{ImageFormat, ImageMode};
use image::{ColorType, DynamicImage, ImageBuffer, Luma, LumaA, Rgb, Rgba};

use crate::BBox;

#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug)]
pub enum CowImage<'a> {
    L(ImageBuffer<Luma<u8>, Cow<'a, [u8]>>),
    LA(ImageBuffer<LumaA<u8>, Cow<'a, [u8]>>),
    RGB(ImageBuffer<Rgb<u8>, Cow<'a, [u8]>>),
    RGBA(ImageBuffer<Rgba<u8>, Cow<'a, [u8]>>),
    L16(ImageBuffer<Luma<u16>, Cow<'a, [u16]>>),
    LA16(ImageBuffer<LumaA<u16>, Cow<'a, [u16]>>),
    RGB16(ImageBuffer<Rgb<u16>, Cow<'a, [u16]>>),
    RGBA16(ImageBuffer<Rgba<u16>, Cow<'a, [u16]>>),
    RGB32F(ImageBuffer<Rgb<f32>, Cow<'a, [f32]>>),
    RGBA32F(ImageBuffer<Rgba<f32>, Cow<'a, [f32]>>),
}

macro_rules! with_method_on_image_buffer {
    (
    $key_type:expr, $method: ident
) => {{
        match $key_type {
            CowImage::L(img) => img.$method(),
            CowImage::LA(img) => img.$method(),
            CowImage::RGB(img) => img.$method(),
            CowImage::RGBA(img) => img.$method(),
            CowImage::L16(img) => img.$method(),
            CowImage::LA16(img) => img.$method(),
            CowImage::RGB16(img) => img.$method(),
            CowImage::RGBA16(img) => img.$method(),
            CowImage::RGB32F(img) => img.$method(),
            CowImage::RGBA32F(img) => img.$method(),
        }
    }};
}

impl<'a> CowImage<'a> {
    pub fn from_raw(mode: &ImageMode, width: u32, height: u32, data: Cow<'a, [u8]>) -> Self {
        use CowImage::{L, LA, RGB, RGBA};
        match mode {
            ImageMode::L => L(ImageBuffer::from_raw(width, height, data).unwrap()),
            ImageMode::LA => LA(ImageBuffer::from_raw(width, height, data).unwrap()),
            ImageMode::RGB => RGB(ImageBuffer::from_raw(width, height, data).unwrap()),
            ImageMode::RGBA => RGBA(ImageBuffer::from_raw(width, height, data).unwrap()),
            _ => unimplemented!("{mode} is currently not implemented!"),
        }
    }
    pub fn height(&self) -> u32 {
        with_method_on_image_buffer!(self, height)
    }

    pub fn width(&self) -> u32 {
        with_method_on_image_buffer!(self, width)
    }

    pub fn as_u8_slice(&self) -> &[u8] {
        use CowImage::{L, LA, RGB, RGBA};
        match self {
            L(img) => img.as_raw(),
            LA(img) => img.as_raw(),
            RGB(img) => img.as_raw(),
            RGBA(img) => img.as_raw(),
            _ => unimplemented!("unimplemented {self:?}"),
        }
    }
    pub fn mode(&self) -> ImageMode {
        use CowImage::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

        match self {
            L(..) => ImageMode::L,
            LA(..) => ImageMode::LA,
            RGB(..) => ImageMode::RGB,
            RGBA(..) => ImageMode::RGBA,
            L16(..) => ImageMode::L16,
            LA16(..) => ImageMode::LA16,
            RGB16(..) => ImageMode::RGB16,
            RGBA16(..) => ImageMode::RGBA16,
            RGB32F(..) => ImageMode::RGB32F,
            RGBA32F(..) => ImageMode::RGBA32F,
        }
    }
    pub fn color(&self) -> ColorType {
        let mode = CowImage::mode(self);
        use ImageMode::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};
        match mode {
            L => ColorType::L8,
            LA => ColorType::La8,
            RGB => ColorType::Rgb8,
            RGBA => ColorType::Rgba8,
            L16 => ColorType::L16,
            LA16 => ColorType::La16,
            RGB16 => ColorType::Rgb16,
            RGBA16 => ColorType::Rgba16,
            RGB32F => ColorType::Rgb32F,
            RGBA32F => ColorType::Rgba32F,
        }
    }

    pub fn decode(bytes: &[u8]) -> DaftResult<Self> {
        image::load_from_memory(bytes)
            .map(std::convert::Into::into)
            .map_err(|e| DaftError::ValueError(format!("Decoding image from bytes failed: {e}")))
    }

    pub fn encode<W>(&self, image_format: ImageFormat, writer: &mut W) -> DaftResult<()>
    where
        W: Write + Seek,
    {
        image::write_buffer_with_format(
            writer,
            self.as_u8_slice(),
            self.width(),
            self.height(),
            self.color(),
            convert_img_fmt(image_format),
        )
        .map_err(|e| {
            DaftError::ValueError(format!(
                "Encoding image into file format {image_format} failed: {e}"
            ))
        })
    }

    pub fn fit_to(&self, w: u32, h: u32) -> Self {
        // Preserving aspect ratio, resize an image to fit within the specified dimensions.
        let scale_factor = {
            let width_scale = w as f64 / self.width() as f64;
            let height_scale = h as f64 / self.height() as f64;
            width_scale.min(height_scale)
        };
        let new_w = self.width() as f64 * scale_factor;
        let new_h = self.height() as f64 * scale_factor;

        self.resize(new_w.floor() as u32, new_h.floor() as u32)
    }

    pub fn resize(&self, w: u32, h: u32) -> Self {
        use CowImage::{L, LA, RGB, RGBA};
        match self {
            L(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                CowImage::L(image_buffer_vec_to_cow(result))
            }
            LA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                CowImage::LA(image_buffer_vec_to_cow(result))
            }
            RGB(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                CowImage::RGB(image_buffer_vec_to_cow(result))
            }
            RGBA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                CowImage::RGBA(image_buffer_vec_to_cow(result))
            }
            _ => unimplemented!("Mode {self:?} not implemented"),
        }
    }

    pub fn crop(&self, bbox: &BBox) -> Self {
        // HACK(jay): The `.to_image()` method on SubImage takes in `'static` references for some reason
        // This hack will ensure that `&self` adheres to that overly prescriptive bound
        let inner = unsafe { std::mem::transmute::<&CowImage<'a>, &CowImage<'static>>(self) };
        match inner {
            CowImage::L(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                CowImage::L(image_buffer_vec_to_cow(result))
            }
            CowImage::LA(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                CowImage::LA(image_buffer_vec_to_cow(result))
            }
            CowImage::RGB(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                CowImage::RGB(image_buffer_vec_to_cow(result))
            }
            CowImage::RGBA(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                CowImage::RGBA(image_buffer_vec_to_cow(result))
            }
            _ => unimplemented!("Mode {self:?} not implemented"),
        }
    }

    pub fn into_mode(self, mode: ImageMode) -> Self {
        let img: DynamicImage = self.into();
        // I couldn't find a method from the image crate to do this
        let img: DynamicImage = match mode {
            ImageMode::L => img.into_luma8().into(),
            ImageMode::LA => img.into_luma_alpha8().into(),
            ImageMode::RGB => img.into_rgb8().into(),
            ImageMode::RGBA => img.into_rgba8().into(),
            ImageMode::L16 => img.into_luma16().into(),
            ImageMode::LA16 => img.into_luma_alpha16().into(),
            ImageMode::RGB16 => img.into_rgb16().into(),
            ImageMode::RGBA16 => img.into_rgba16().into(),
            ImageMode::RGB32F => img.into_rgb32f().into(),
            ImageMode::RGBA32F => img.into_rgba32f().into(),
        };
        img.into()
    }

    fn to_grayscale_resized(&self, w: u32, h: u32) -> ImageBuffer<Luma<u8>, Vec<u8>> {
        let dyn_img: DynamicImage = self.clone_to_dynamic_image();
        let gray = dyn_img.into_luma8();
        image::imageops::resize(&gray, w, h, image::imageops::FilterType::Nearest)
    }

    fn clone_to_dynamic_image(&self) -> DynamicImage {
        match self {
            CowImage::L(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::LA(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGB(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGBA(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::L16(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::LA16(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGB16(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGBA16(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGB32F(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
            CowImage::RGBA32F(buf) => image_buffer_cow_to_vec(buf.clone()).into(),
        }
    }

    pub fn average_hash(&self) -> [u8; 8] {
        let resized = self.to_grayscale_resized(8, 8);
        let pixels: Vec<u8> = resized.pixels().map(|p| p.0[0]).collect();
        let mean: u64 = pixels.iter().map(|&p| p as u64).sum::<u64>() / 64;
        let mut hash = [0u8; 8];
        for (i, &pixel) in pixels.iter().enumerate() {
            if pixel as u64 >= mean {
                hash[i / 8] |= 1 << (7 - (i % 8));
            }
        }
        hash
    }

    pub fn difference_hash(&self) -> [u8; 8] {
        let resized = self.to_grayscale_resized(9, 8);
        let mut hash = [0u8; 8];
        for y in 0..8u32 {
            for x in 0..8u32 {
                let left = resized.get_pixel(x, y).0[0];
                let right = resized.get_pixel(x + 1, y).0[0];
                if left > right {
                    let bit_idx = (y * 8 + x) as usize;
                    hash[bit_idx / 8] |= 1 << (7 - (bit_idx % 8));
                }
            }
        }
        hash
    }

    pub fn perceptual_hash(&self) -> [u8; 8] {
        let resized = self.to_grayscale_resized(32, 32);
        let pixels: Vec<f64> = resized.pixels().map(|p| p.0[0] as f64).collect();

        // Compute 2D DCT-II on the 32x32 image.
        let dct = dct2d_32x32(&pixels);

        // Extract the top-left 8x8 block (excluding DC at [0][0]).
        let mut low_freq = Vec::with_capacity(64);
        for row in 0..8usize {
            for col in 0..8usize {
                if row == 0 && col == 0 {
                    continue;
                }
                low_freq.push(dct[row * 32 + col]);
            }
        }

        // Compute median of the 63 low-frequency coefficients.
        let mut sorted = low_freq.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];

        // Generate hash: compare each of the 64 coefficients (including DC) to median.
        let mut hash = [0u8; 8];
        for row in 0..8usize {
            for col in 0..8usize {
                let idx = row * 8 + col;
                if dct[row * 32 + col] > median {
                    hash[idx / 8] |= 1 << (7 - (idx % 8));
                }
            }
        }
        hash
    }

    pub fn wavelet_hash(&self) -> [u8; 8] {
        let resized = self.to_grayscale_resized(8, 8);
        let mut data: Vec<f64> = resized.pixels().map(|p| p.0[0] as f64).collect();

        // Apply 2D Haar wavelet transform (one level).
        haar_2d(&mut data, 8);

        // Compute median of the wavelet coefficients.
        let mut sorted = data.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];

        // Generate hash.
        let mut hash = [0u8; 8];
        for (i, &val) in data.iter().enumerate() {
            if val > median {
                hash[i / 8] |= 1 << (7 - (i % 8));
            }
        }
        hash
    }

    pub fn crop_resistant_hash(&self) -> Vec<[u8; 8]> {
        let dyn_img = self.clone_to_dynamic_image();
        let (w, h) = (dyn_img.width(), dyn_img.height());

        let seg_w = w / 2;
        let seg_h = h / 2;

        if seg_w == 0 || seg_h == 0 {
            // Image too small to segment; return a single dHash.
            return vec![self.difference_hash()];
        }

        let offsets_x = [0, w / 4, w / 2];
        let offsets_y = [0, h / 4, h / 2];

        let mut hashes = Vec::new();
        for &oy in &offsets_y {
            for &ox in &offsets_x {
                let crop_w = seg_w.min(w - ox);
                let crop_h = seg_h.min(h - oy);
                if crop_w < 2 || crop_h < 2 {
                    continue;
                }
                let segment = dyn_img.crop_imm(ox, oy, crop_w, crop_h);
                let cow: CowImage = segment.into();
                hashes.push(cow.difference_hash());
            }
        }
        hashes
    }
}

fn image_buffer_vec_to_cow<'a, P, T>(input: ImageBuffer<P, Vec<T>>) -> ImageBuffer<P, Cow<'a, [T]>>
where
    P: image::Pixel<Subpixel = T>,
    Vec<T>: Deref<Target = [P::Subpixel]>,
    T: ToOwned + std::clone::Clone,
    [T]: ToOwned,
{
    let h = input.height();
    let w = input.width();
    let owned: Cow<[T]> = input.into_raw().into();
    ImageBuffer::from_raw(w, h, owned).unwrap()
}

fn image_buffer_vec_ref_to_cow<P, T>(input: &ImageBuffer<P, Vec<T>>) -> ImageBuffer<P, Cow<'_, [T]>>
where
    P: image::Pixel<Subpixel = T>,
    Vec<T>: Deref<Target = [P::Subpixel]>,
    T: ToOwned + std::clone::Clone,
    [T]: ToOwned,
{
    let h = input.height();
    let w = input.width();
    let owned: Cow<[T]> = input.as_raw().into();
    ImageBuffer::from_raw(w, h, owned).unwrap()
}

fn image_buffer_cow_to_vec<P, T>(input: ImageBuffer<P, Cow<[T]>>) -> ImageBuffer<P, Vec<T>>
where
    P: image::Pixel<Subpixel = T>,
    Vec<T>: Deref<Target = [P::Subpixel]>,
    T: ToOwned + std::clone::Clone,
    [T]: ToOwned,
{
    let h = input.height();
    let w = input.width();
    let owned: Vec<T> = input.into_raw().to_vec();
    ImageBuffer::from_raw(w, h, owned).unwrap()
}

impl<'a> From<DynamicImage> for CowImage<'a> {
    fn from(dyn_img: DynamicImage) -> Self {
        match dyn_img {
            DynamicImage::ImageLuma8(img_buf) => {
                CowImage::<'a>::L(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA8(img_buf) => {
                CowImage::<'a>::LA(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb8(img_buf) => {
                CowImage::<'a>::RGB(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba8(img_buf) => {
                CowImage::<'a>::RGBA(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLuma16(img_buf) => {
                CowImage::<'a>::L16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA16(img_buf) => {
                CowImage::<'a>::LA16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb16(img_buf) => {
                CowImage::<'a>::RGB16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba16(img_buf) => {
                CowImage::<'a>::RGBA16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb32F(img_buf) => {
                CowImage::<'a>::RGB32F(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba32F(img_buf) => {
                CowImage::<'a>::RGBA32F(image_buffer_vec_to_cow(img_buf))
            }
            _ => unimplemented!("{dyn_img:?} not implemented"),
        }
    }
}

impl<'a> From<&'a DynamicImage> for CowImage<'a> {
    fn from(dyn_img: &'a DynamicImage) -> Self {
        match dyn_img {
            DynamicImage::ImageLuma8(img_buf) => {
                CowImage::<'a>::L(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA8(img_buf) => {
                CowImage::<'a>::LA(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb8(img_buf) => {
                CowImage::<'a>::RGB(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba8(img_buf) => {
                CowImage::<'a>::RGBA(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLuma16(img_buf) => {
                CowImage::<'a>::L16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA16(img_buf) => {
                CowImage::<'a>::LA16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb16(img_buf) => {
                CowImage::<'a>::RGB16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba16(img_buf) => {
                CowImage::<'a>::RGBA16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb32F(img_buf) => {
                CowImage::<'a>::RGB32F(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba32F(img_buf) => {
                CowImage::<'a>::RGBA32F(image_buffer_vec_ref_to_cow(img_buf))
            }
            _ => unimplemented!("{dyn_img:?} not implemented"),
        }
    }
}

impl<'a> From<CowImage<'a>> for DynamicImage {
    fn from(daft_buf: CowImage<'a>) -> Self {
        match daft_buf {
            CowImage::L(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::LA(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGB(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGBA(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::L16(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::LA16(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGB16(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGBA16(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGB32F(buf) => image_buffer_cow_to_vec(buf).into(),
            CowImage::RGBA32F(buf) => image_buffer_cow_to_vec(buf).into(),
        }
    }
}

fn convert_img_fmt(fmt: ImageFormat) -> image::ImageFormat {
    match fmt {
        ImageFormat::PNG => image::ImageFormat::Png,
        ImageFormat::JPEG => image::ImageFormat::Jpeg,
        ImageFormat::TIFF => image::ImageFormat::Tiff,
        ImageFormat::GIF => image::ImageFormat::Gif,
        ImageFormat::BMP => image::ImageFormat::Bmp,
    }
}

fn dct2d_32x32(input: &[f64]) -> Vec<f64> {
    const N: usize = 32;
    assert_eq!(input.len(), N * N);

    // Precompute cosine table.
    let mut cos_table = vec![0.0f64; N * N];
    for k in 0..N {
        for n in 0..N {
            cos_table[k * N + n] =
                (std::f64::consts::PI * (2 * n + 1) as f64 * k as f64 / (2 * N) as f64).cos();
        }
    }

    // Apply 1D DCT-II on rows.
    let mut row_dct = vec![0.0f64; N * N];
    for row in 0..N {
        for k in 0..N {
            let mut sum = 0.0;
            for n in 0..N {
                sum += input[row * N + n] * cos_table[k * N + n];
            }
            row_dct[row * N + k] = sum;
        }
    }

    // Apply 1D DCT-II on columns.
    let mut result = vec![0.0f64; N * N];
    for col in 0..N {
        for k in 0..N {
            let mut sum = 0.0;
            for n in 0..N {
                sum += row_dct[n * N + col] * cos_table[k * N + n];
            }
            result[k * N + col] = sum;
        }
    }

    result
}

fn haar_2d(data: &mut [f64], n: usize) {
    assert!(data.len() == n * n);
    let mut temp = vec![0.0f64; n];

    // Transform rows.
    for row in 0..n {
        let half = n / 2;
        for i in 0..half {
            let a = data[row * n + 2 * i];
            let b = data[row * n + 2 * i + 1];
            temp[i] = f64::midpoint(a, b);
            temp[half + i] = (a - b) / 2.0;
        }
        data[row * n..row * n + n].copy_from_slice(&temp[..n]);
    }

    // Transform columns.
    let mut col_data = vec![0.0f64; n];
    for col in 0..n {
        for row in 0..n {
            col_data[row] = data[row * n + col];
        }
        let half = n / 2;
        for i in 0..half {
            let a = col_data[2 * i];
            let b = col_data[2 * i + 1];
            temp[i] = f64::midpoint(a, b);
            temp[half + i] = (a - b) / 2.0;
        }
        for row in 0..n {
            data[row * n + col] = temp[row];
        }
    }
}
