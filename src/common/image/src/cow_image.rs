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
