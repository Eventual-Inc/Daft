use std::{
    borrow::Cow,
    hash::Hash,
    io::{Seek, Write},
    ops::Deref,
};

use common_error::{DaftError, DaftResult};
use image::{ColorType, DynamicImage, ImageBuffer, Luma, LumaA, Rgb, Rgba};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{array::image_array::BBox, prelude::*, series::IntoSeries};

#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug)]
pub enum DaftImageBuffer<'a> {
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
        use DaftImageBuffer::*;

        match $key_type {
            L(img) => img.$method(),
            LA(img) => img.$method(),
            RGB(img) => img.$method(),
            RGBA(img) => img.$method(),
            L16(img) => img.$method(),
            LA16(img) => img.$method(),
            RGB16(img) => img.$method(),
            RGBA16(img) => img.$method(),
            RGB32F(img) => img.$method(),
            RGBA32F(img) => img.$method(),
        }
    }};
}

impl<'a> DaftImageBuffer<'a> {
    pub fn from_raw(mode: &ImageMode, width: u32, height: u32, data: Cow<'a, [u8]>) -> Self {
        use DaftImageBuffer::{L, LA, RGB, RGBA};
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
        use DaftImageBuffer::{L, LA, RGB, RGBA};
        match self {
            L(img) => img.as_raw(),
            LA(img) => img.as_raw(),
            RGB(img) => img.as_raw(),
            RGBA(img) => img.as_raw(),
            _ => unimplemented!("unimplemented {self:?}"),
        }
    }
    pub fn mode(&self) -> ImageMode {
        use DaftImageBuffer::{L, L16, LA, LA16, RGB, RGB16, RGB32F, RGBA, RGBA16, RGBA32F};

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
        let mode = DaftImageBuffer::mode(self);
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
        use DaftImageBuffer::{L, LA, RGB, RGBA};
        match self {
            L(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::L(image_buffer_vec_to_cow(result))
            }
            LA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::LA(image_buffer_vec_to_cow(result))
            }
            RGB(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::RGB(image_buffer_vec_to_cow(result))
            }
            RGBA(imgbuf) => {
                let result =
                    image::imageops::resize(imgbuf, w, h, image::imageops::FilterType::Triangle);
                DaftImageBuffer::RGBA(image_buffer_vec_to_cow(result))
            }
            _ => unimplemented!("Mode {self:?} not implemented"),
        }
    }

    pub fn crop(&self, bbox: &BBox) -> Self {
        // HACK(jay): The `.to_image()` method on SubImage takes in `'static` references for some reason
        // This hack will ensure that `&self` adheres to that overly prescriptive bound
        let inner =
            unsafe { std::mem::transmute::<&DaftImageBuffer<'a>, &DaftImageBuffer<'static>>(self) };
        match inner {
            DaftImageBuffer::L(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                DaftImageBuffer::L(image_buffer_vec_to_cow(result))
            }
            DaftImageBuffer::LA(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                DaftImageBuffer::LA(image_buffer_vec_to_cow(result))
            }
            DaftImageBuffer::RGB(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                DaftImageBuffer::RGB(image_buffer_vec_to_cow(result))
            }
            DaftImageBuffer::RGBA(imgbuf) => {
                let result =
                    image::imageops::crop_imm(imgbuf, bbox.0, bbox.1, bbox.2, bbox.3).to_image();
                DaftImageBuffer::RGBA(image_buffer_vec_to_cow(result))
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

fn image_buffer_vec_ref_to_cow<P, T>(input: &ImageBuffer<P, Vec<T>>) -> ImageBuffer<P, Cow<[T]>>
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

impl<'a> From<DynamicImage> for DaftImageBuffer<'a> {
    fn from(dyn_img: DynamicImage) -> Self {
        match dyn_img {
            DynamicImage::ImageLuma8(img_buf) => {
                DaftImageBuffer::<'a>::L(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA8(img_buf) => {
                DaftImageBuffer::<'a>::LA(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb8(img_buf) => {
                DaftImageBuffer::<'a>::RGB(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba8(img_buf) => {
                DaftImageBuffer::<'a>::RGBA(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLuma16(img_buf) => {
                DaftImageBuffer::<'a>::L16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA16(img_buf) => {
                DaftImageBuffer::<'a>::LA16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb16(img_buf) => {
                DaftImageBuffer::<'a>::RGB16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba16(img_buf) => {
                DaftImageBuffer::<'a>::RGBA16(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgb32F(img_buf) => {
                DaftImageBuffer::<'a>::RGB32F(image_buffer_vec_to_cow(img_buf))
            }
            DynamicImage::ImageRgba32F(img_buf) => {
                DaftImageBuffer::<'a>::RGBA32F(image_buffer_vec_to_cow(img_buf))
            }
            _ => unimplemented!("{dyn_img:?} not implemented"),
        }
    }
}

impl<'a> From<&'a DynamicImage> for DaftImageBuffer<'a> {
    fn from(dyn_img: &'a DynamicImage) -> Self {
        match dyn_img {
            DynamicImage::ImageLuma8(img_buf) => {
                DaftImageBuffer::<'a>::L(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA8(img_buf) => {
                DaftImageBuffer::<'a>::LA(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb8(img_buf) => {
                DaftImageBuffer::<'a>::RGB(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba8(img_buf) => {
                DaftImageBuffer::<'a>::RGBA(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLuma16(img_buf) => {
                DaftImageBuffer::<'a>::L16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageLumaA16(img_buf) => {
                DaftImageBuffer::<'a>::LA16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb16(img_buf) => {
                DaftImageBuffer::<'a>::RGB16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba16(img_buf) => {
                DaftImageBuffer::<'a>::RGBA16(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgb32F(img_buf) => {
                DaftImageBuffer::<'a>::RGB32F(image_buffer_vec_ref_to_cow(img_buf))
            }
            DynamicImage::ImageRgba32F(img_buf) => {
                DaftImageBuffer::<'a>::RGBA32F(image_buffer_vec_ref_to_cow(img_buf))
            }
            _ => unimplemented!("{dyn_img:?} not implemented"),
        }
    }
}

impl<'a> From<DaftImageBuffer<'a>> for DynamicImage {
    fn from(daft_buf: DaftImageBuffer<'a>) -> Self {
        match daft_buf {
            DaftImageBuffer::L(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::LA(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGB(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGBA(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::L16(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::LA16(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGB16(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGBA16(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGB32F(buf) => image_buffer_cow_to_vec(buf).into(),
            DaftImageBuffer::RGBA32F(buf) => image_buffer_cow_to_vec(buf).into(),
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

/// Wrapper around image::DynamicImage to implement certain traits
#[derive(Debug, Clone, PartialEq)]
pub struct ImageBufferWrapper(pub DynamicImage);

impl Hash for ImageBufferWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            DynamicImage::ImageLuma8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLumaA8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgba8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLuma16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLumaA16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgba16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb32F(image_buffer) => {
                image_buffer.width().hash(state);
                image_buffer.height().hash(state);
                let as_series =
                    Float32Array::from(("image", image_buffer.as_raw().as_slice())).into_series();
                Hash::hash(&as_series, state);
            }
            DynamicImage::ImageRgba32F(image_buffer) => {
                image_buffer.width().hash(state);
                image_buffer.height().hash(state);
                let as_series =
                    Float32Array::from(("image", image_buffer.as_raw().as_slice())).into_series();
                Hash::hash(&as_series, state);
            }
            _ => todo!(),
        }
    }
}

// Helper struct for serialization
#[derive(Serialize, Deserialize)]
struct SerializedImageBufferWrapper {
    variant: u8,
    width: u32,
    height: u32,
    data: Vec<u8>,
}

impl Serialize for ImageBufferWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (variant, width, height, data) = match &self.0 {
            DynamicImage::ImageLuma8(img) => (0u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageLumaA8(img) => {
                (1u8, img.width(), img.height(), img.as_raw().clone())
            }
            DynamicImage::ImageRgb8(img) => (2u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageRgba8(img) => (3u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageLuma16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (4u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageLumaA16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (5u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgb16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (6u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgba16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (7u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgb32F(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (8u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgba32F(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (9u8, img.width(), img.height(), bytes)
            }
            _ => {
                return Err(serde::ser::Error::custom(
                    "Unsupported DynamicImage variant",
                ))
            }
        };

        let serialized = SerializedImageBufferWrapper {
            variant,
            width,
            height,
            data,
        };

        serialized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ImageBufferWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = SerializedImageBufferWrapper::deserialize(deserializer)?;

        let dynamic_image = match serialized.variant {
            0 => {
                // ImageLuma8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageLuma8 from raw data")
                        })?;
                DynamicImage::ImageLuma8(img)
            }
            1 => {
                // ImageLumaA8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageLumaA8 from raw data")
                        })?;
                DynamicImage::ImageLumaA8(img)
            }
            2 => {
                // ImageRgb8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageRgb8 from raw data")
                        })?;
                DynamicImage::ImageRgb8(img)
            }
            3 => {
                // ImageRgba8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageRgba8 from raw data")
                        })?;
                DynamicImage::ImageRgba8(img)
            }
            4 => {
                // ImageLuma16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageLuma16 from raw data")
                    })?;
                DynamicImage::ImageLuma16(img)
            }
            5 => {
                // ImageLumaA16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageLumaA16 from raw data")
                    })?;
                DynamicImage::ImageLumaA16(img)
            }
            6 => {
                // ImageRgb16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgb16 from raw data")
                    })?;
                DynamicImage::ImageRgb16(img)
            }
            7 => {
                // ImageRgba16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgba16 from raw data")
                    })?;
                DynamicImage::ImageRgba16(img)
            }
            8 => {
                // ImageRgb32F
                if serialized.data.len() % 4 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for f32 pixels",
                    ));
                }
                let f32_data: Vec<f32> = serialized
                    .data
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, f32_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgb32F from raw data")
                    })?;
                DynamicImage::ImageRgb32F(img)
            }
            9 => {
                // ImageRgba32F
                if serialized.data.len() % 4 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for f32 pixels",
                    ));
                }
                let f32_data: Vec<f32> = serialized
                    .data
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, f32_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgba32F from raw data")
                    })?;
                DynamicImage::ImageRgba32F(img)
            }
            _ => return Err(serde::de::Error::custom("Unknown DynamicImage variant")),
        };

        Ok(Self(dynamic_image))
    }
}

#[cfg(test)]
mod tests {
    use image::{Luma, Rgb, Rgba};

    use super::*;

    #[test]
    fn test_serialize_deserialize_luma8() {
        // Create a simple 2x2 grayscale image
        let data = vec![0u8, 64, 128, 255];
        let img = ImageBuffer::<Luma<u8>, Vec<u8>>::from_raw(2, 2, data.clone()).unwrap();
        let wrapper = ImageBufferWrapper(DynamicImage::ImageLuma8(img));

        // Serialize
        let serialized = bincode::serialize(&wrapper).unwrap();

        // Deserialize
        let deserialized: ImageBufferWrapper = bincode::deserialize(&serialized).unwrap();

        // Check that it matches the original
        if let DynamicImage::ImageLuma8(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (2, 2));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageLuma8 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgb8() {
        // Create a simple 2x1 RGB image
        let data = vec![255u8, 0, 0, 0, 255, 0]; // Red pixel, Green pixel
        let img = ImageBuffer::<Rgb<u8>, Vec<u8>>::from_raw(2, 1, data.clone()).unwrap();
        let wrapper = ImageBufferWrapper(DynamicImage::ImageRgb8(img));

        // Serialize
        let serialized = bincode::serialize(&wrapper).unwrap();

        // Deserialize
        let deserialized: ImageBufferWrapper = bincode::deserialize(&serialized).unwrap();

        // Check that it matches the original
        if let DynamicImage::ImageRgb8(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (2, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgb8 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgba16() {
        // Create a simple 1x1 RGBA16 image
        let data = vec![65535u16, 32768, 16384, 8192]; // R, G, B, A
        let img = ImageBuffer::<Rgba<u16>, Vec<u16>>::from_raw(1, 1, data.clone()).unwrap();
        let wrapper = ImageBufferWrapper(DynamicImage::ImageRgba16(img));

        // Serialize
        let serialized = bincode::serialize(&wrapper).unwrap();

        // Deserialize
        let deserialized: ImageBufferWrapper = bincode::deserialize(&serialized).unwrap();

        // Check that it matches the original
        if let DynamicImage::ImageRgba16(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (1, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgba16 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgb32f() {
        // Create a simple 1x1 RGB32F image
        let data = vec![1.0f32, 0.5, 0.0]; // R, G, B
        let img = ImageBuffer::<Rgb<f32>, Vec<f32>>::from_raw(1, 1, data.clone()).unwrap();
        let wrapper = ImageBufferWrapper(DynamicImage::ImageRgb32F(img));

        // Serialize
        let serialized = bincode::serialize(&wrapper).unwrap();

        // Deserialize
        let deserialized: ImageBufferWrapper = bincode::deserialize(&serialized).unwrap();

        // Check that it matches the original
        if let DynamicImage::ImageRgb32F(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (1, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgb32F variant");
        }
    }

    #[test]
    fn test_roundtrip_equality() {
        // Test that serialization -> deserialization preserves equality
        let data = vec![100u8, 150, 200, 255];
        let img = ImageBuffer::<Rgba<u8>, Vec<u8>>::from_raw(1, 1, data).unwrap();
        let original = ImageBufferWrapper(DynamicImage::ImageRgba8(img));

        let serialized = bincode::serialize(&original).unwrap();
        let deserialized: ImageBufferWrapper = bincode::deserialize(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }
}
