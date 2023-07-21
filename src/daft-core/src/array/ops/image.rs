use std::borrow::Cow;
use std::io::{Seek, SeekFrom, Write};
use std::vec;

use image::{ColorType, DynamicImage, ImageBuffer};

use crate::datatypes::{
    logical::{DaftImageryType, FixedShapeImageArray, ImageArray, LogicalArray},
    BinaryArray, DaftLogicalType, DataType, Field, FixedSizeListArray, ImageFormat, ImageMode,
    StructArray,
};
use common_error::{DaftError, DaftResult};
use image::{Luma, LumaA, Rgb, Rgba};

use super::as_arrow::AsArrow;
use num_traits::FromPrimitive;

use std::ops::Deref;

#[derive(Clone)]
pub struct BBox(u32, u32, u32, u32);

impl BBox {
    pub fn from_u32_arrow_array(arr: Box<dyn arrow2::array::Array>) -> Self {
        assert!(arr.len() == 4);
        let mut iter = arr
            .as_any()
            .downcast_ref::<arrow2::array::UInt32Array>()
            .unwrap()
            .iter();
        BBox(
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
            *iter.next().unwrap().unwrap(),
        )
    }
}

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

type IOResult<T = (), E = std::io::Error> = std::result::Result<T, E>;

/// A wrapper of a writer that tracks the number of bytes successfully written.
pub struct CountingWriter<W> {
    inner: W,
    count: u64,
}

impl<W> CountingWriter<W> {
    /// The number of bytes successfull written so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Extracts the inner writer, discarding this wrapper.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W> From<W> for CountingWriter<W> {
    fn from(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W: Write + std::fmt::Debug> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        let written = self.inner.write(buf)?;
        self.count += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> IOResult {
        self.inner.flush()
    }
}

impl<W: Write + Seek> Seek for CountingWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        self.inner.seek(pos)
    }
}

impl<'a> DaftImageBuffer<'a> {
    pub fn height(&self) -> u32 {
        with_method_on_image_buffer!(self, height)
    }

    pub fn width(&self) -> u32 {
        with_method_on_image_buffer!(self, width)
    }

    pub fn as_u8_slice(&'a self) -> &'a [u8] {
        use DaftImageBuffer::*;
        match self {
            L(img) => img.as_raw(),
            LA(img) => img.as_raw(),
            RGB(img) => img.as_raw(),
            RGBA(img) => img.as_raw(),
            _ => unimplemented!("unimplemented {self:?}"),
        }
    }

    pub fn color(&self) -> ColorType {
        self.mode().into()
    }

    pub fn mode(&self) -> ImageMode {
        use DaftImageBuffer::*;

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

    pub fn decode(bytes: &[u8]) -> DaftResult<Self> {
        image::load_from_memory(bytes)
            .map(|v| v.into())
            .map_err(|e| DaftError::ValueError(format!("Decoding image from bytes failed: {}", e)))
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
            image::ImageFormat::from(image_format),
        )
        .map_err(|e| {
            DaftError::ValueError(format!(
                "Encoding image into file format {} failed: {}",
                image_format, e
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
        use DaftImageBuffer::*;
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

pub struct ImageArraySidecarData {
    pub channels: Vec<u16>,
    pub heights: Vec<u32>,
    pub widths: Vec<u32>,
    pub modes: Vec<u8>,
    pub validity: Option<arrow2::bitmap::Bitmap>,
}

pub trait AsImageObj {
    fn as_image_obj(&self, idx: usize) -> Option<DaftImageBuffer<'_>>;
}

pub struct ImageBufferIter<'a, T>
where
    T: DaftLogicalType + DaftImageryType,
{
    cursor: usize,
    image_array: &'a LogicalArray<T>,
}

impl<'a, T> ImageBufferIter<'a, T>
where
    T: DaftLogicalType + DaftImageryType,
{
    pub fn new(image_array: &'a LogicalArray<T>) -> Self {
        Self {
            cursor: 0usize,
            image_array,
        }
    }
}

impl<'a, T> Iterator for ImageBufferIter<'a, T>
where
    T: DaftLogicalType + DaftImageryType,
    LogicalArray<T>: AsImageObj,
{
    type Item = Option<DaftImageBuffer<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.image_array.len() {
            None
        } else {
            let image_obj = self.image_array.as_image_obj(self.cursor);
            self.cursor += 1;
            Some(image_obj)
        }
    }
}

impl ImageArray {
    pub fn image_mode(&self) -> &Option<ImageMode> {
        match self.logical_type() {
            DataType::Image(mode) => mode,
            _ => panic!("Expected dtype to be Image"),
        }
    }

    pub fn data_array(&self) -> &arrow2::array::ListArray<i64> {
        let p = self.physical.as_arrow();
        const IMAGE_DATA_IDX: usize = 0;
        let array = p.values().get(IMAGE_DATA_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn channel_array(&self) -> &arrow2::array::UInt16Array {
        let p = self.physical.as_arrow();
        const IMAGE_CHANNEL_IDX: usize = 1;
        let array = p.values().get(IMAGE_CHANNEL_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn height_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_HEIGHT_IDX: usize = 2;
        let array = p.values().get(IMAGE_HEIGHT_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn width_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_WIDTH_IDX: usize = 3;
        let array = p.values().get(IMAGE_WIDTH_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn mode_array(&self) -> &arrow2::array::UInt8Array {
        let p = self.physical.as_arrow();
        const IMAGE_MODE_IDX: usize = 4;
        let array = p.values().get(IMAGE_MODE_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn from_vecs<T: arrow2::types::NativeType>(
        name: &str,
        data_type: DataType,
        data: Vec<T>,
        offsets: Vec<i64>,
        sidecar_data: ImageArraySidecarData,
    ) -> DaftResult<Self> {
        if data.is_empty() {
            // Create an all-null array if the data array is empty.
            let physical_type = data_type.to_physical();
            let null_struct_array = arrow2::array::new_null_array(
                physical_type.to_arrow()?,
                sidecar_data.channels.len(),
            );
            let daft_struct_array =
                StructArray::new(Field::new(name, physical_type).into(), null_struct_array)?;
            return Ok(ImageArray::new(
                Field::new(name, data_type),
                daft_struct_array,
            ));
        }
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let arrow_dtype: arrow2::datatypes::DataType = T::PRIMITIVE.into();
        if let DataType::Image(Some(mode)) = &data_type {
            if mode.get_dtype().to_arrow()? != arrow_dtype {
                panic!("Inner value dtype of provided dtype {data_type:?} is inconsistent with inferred value dtype {arrow_dtype:?}");
            }
        }

        let list_datatype = arrow2::datatypes::DataType::LargeList(Box::new(
            arrow2::datatypes::Field::new("data", arrow_dtype, true),
        ));
        let data_array = Box::new(arrow2::array::ListArray::<i64>::new(
            list_datatype,
            offsets,
            Box::new(arrow2::array::PrimitiveArray::from_vec(data)),
            sidecar_data.validity.clone(),
        ));

        Self::from_list_array(name, data_type, data_array, sidecar_data)
    }

    pub fn from_list_array(
        name: &str,
        data_type: DataType,
        data_array: Box<arrow2::array::ListArray<i64>>,
        sidecar_data: ImageArraySidecarData,
    ) -> DaftResult<Self> {
        let values: Vec<Box<dyn arrow2::array::Array>> = vec![
            data_array,
            Box::new(
                arrow2::array::UInt16Array::from_vec(sidecar_data.channels)
                    .with_validity(sidecar_data.validity.clone()),
            ),
            Box::new(
                arrow2::array::UInt32Array::from_vec(sidecar_data.heights)
                    .with_validity(sidecar_data.validity.clone()),
            ),
            Box::new(
                arrow2::array::UInt32Array::from_vec(sidecar_data.widths)
                    .with_validity(sidecar_data.validity.clone()),
            ),
            Box::new(
                arrow2::array::UInt8Array::from_vec(sidecar_data.modes)
                    .with_validity(sidecar_data.validity.clone()),
            ),
        ];
        let physical_type = data_type.to_physical();
        let struct_array = Box::new(arrow2::array::StructArray::new(
            physical_type.to_arrow()?,
            values,
            sidecar_data.validity,
        ));

        let daft_struct_array = crate::datatypes::StructArray::new(
            Field::new(name, physical_type).into(),
            struct_array,
        )?;
        Ok(ImageArray::new(
            Field::new(name, data_type),
            daft_struct_array,
        ))
    }

    pub fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray> {
        encode_images(self, image_format)
    }

    pub fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let result = resize_images(self, w, h);
        Self::from_daft_image_buffers(self.name(), result.as_slice(), self.image_mode())
    }

    pub fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray> {
        let mut bboxes_iterator: Box<dyn Iterator<Item = Option<BBox>>> = if bboxes.len() == 1 {
            Box::new(std::iter::repeat(
                bboxes
                    .as_arrow()
                    .get(0)
                    .map(|bbox| BBox::from_u32_arrow_array(bbox)),
            ))
        } else {
            Box::new(
                bboxes
                    .as_arrow()
                    .iter()
                    .map(|bbox| bbox.map(|bbox| BBox::from_u32_arrow_array(bbox))),
            )
        };
        let result = crop_images(self, &mut bboxes_iterator);
        Self::from_daft_image_buffers(self.name(), result.as_slice(), self.image_mode())
    }

    pub fn resize_to_fixed_shape_image_array(
        &self,
        w: u32,
        h: u32,
        mode: &ImageMode,
    ) -> DaftResult<FixedShapeImageArray> {
        let result = resize_images(self, w, h);
        FixedShapeImageArray::from_daft_image_buffers(self.name(), result.as_slice(), mode, h, w)
    }

    pub fn from_daft_image_buffers(
        name: &str,
        inputs: &[Option<DaftImageBuffer<'_>>],
        image_mode: &Option<ImageMode>,
    ) -> DaftResult<Self> {
        use DaftImageBuffer::*;
        let is_all_u8 = inputs
            .iter()
            .filter_map(|b| b.as_ref())
            .all(|b| matches!(b, L(..) | LA(..) | RGB(..) | RGBA(..)));
        assert!(is_all_u8);

        let mut data_ref = Vec::with_capacity(inputs.len());
        let mut heights = Vec::with_capacity(inputs.len());
        let mut channels = Vec::with_capacity(inputs.len());
        let mut modes = Vec::with_capacity(inputs.len());
        let mut widths = Vec::with_capacity(inputs.len());
        let mut offsets = Vec::with_capacity(inputs.len() + 1);
        offsets.push(0i64);
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(inputs.len());

        for ib in inputs {
            validity.push(ib.is_some());
            let (height, width, mode, buffer) = match ib {
                Some(ib) => (ib.height(), ib.width(), ib.mode(), ib.as_u8_slice()),
                None => (0u32, 0u32, ImageMode::L, &[] as &[u8]),
            };
            heights.push(height);
            widths.push(width);
            modes.push(mode as u8);
            channels.push(mode.num_channels());
            data_ref.push(buffer);
            offsets.push(offsets.last().unwrap() + buffer.len() as i64);
        }

        let data = data_ref.concat();
        let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
            0 => None,
            _ => Some(validity.into()),
        };
        Self::from_vecs(
            name,
            DataType::Image(*image_mode),
            data,
            offsets,
            ImageArraySidecarData {
                channels,
                heights,
                widths,
                modes,
                validity,
            },
        )
    }
}

impl AsImageObj for ImageArray {
    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<DaftImageBuffer<'a>> {
        assert!(idx < self.len());
        if !self.physical.is_valid(idx) {
            return None;
        }

        let da = self.data_array();
        let ca = self.channel_array();
        let ha = self.height_array();
        let wa = self.width_array();
        let ma = self.mode_array();

        let offsets = da.offsets();

        let start = *offsets.get(idx).unwrap() as usize;
        let end = *offsets.get(idx + 1).unwrap() as usize;

        let values = da
            .values()
            .as_ref()
            .as_any()
            .downcast_ref::<arrow2::array::UInt8Array>()
            .unwrap();
        let slice_data = Cow::Borrowed(&values.values().as_slice()[start..end] as &'a [u8]);

        let c = ca.value(idx);
        let h = ha.value(idx);
        let w = wa.value(idx);
        let m: ImageMode = ImageMode::from_u8(ma.value(idx)).unwrap();
        assert_eq!(m.num_channels(), c);
        let result = match m {
            ImageMode::L => {
                DaftImageBuffer::<'a>::L(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::LA => {
                DaftImageBuffer::<'a>::LA(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::RGB => {
                DaftImageBuffer::<'a>::RGB(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            ImageMode::RGBA => {
                DaftImageBuffer::<'a>::RGBA(ImageBuffer::from_raw(w, h, slice_data).unwrap())
            }
            _ => unimplemented!("{m} is currently not implemented!"),
        };

        assert_eq!(result.height(), h);
        assert_eq!(result.width(), w);
        Some(result)
    }
}

impl FixedShapeImageArray {
    fn mode(&self) -> ImageMode {
        match &self.field.dtype {
            DataType::FixedShapeImage(mode, _, _) => *mode,
            _ => panic!("FixedShapeImageArray does not have the correct FixedShapeImage dtype"),
        }
    }

    pub fn from_daft_image_buffers(
        name: &str,
        inputs: &[Option<DaftImageBuffer<'_>>],
        image_mode: &ImageMode,
        height: u32,
        width: u32,
    ) -> DaftResult<Self> {
        use DaftImageBuffer::*;
        let is_all_u8 = inputs
            .iter()
            .filter_map(|b| b.as_ref())
            .all(|b| matches!(b, L(..) | LA(..) | RGB(..) | RGBA(..)));
        assert!(is_all_u8);

        let num_channels = image_mode.num_channels();
        let mut data_ref = Vec::with_capacity(inputs.len());
        let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(inputs.len());
        let list_size = (height * width * num_channels as u32) as usize;
        let null_list = vec![0u8; list_size];
        for ib in inputs.iter() {
            validity.push(ib.is_some());
            let buffer = match ib {
                Some(ib) => ib.as_u8_slice(),
                None => null_list.as_slice(),
            };
            data_ref.push(buffer)
        }
        let data = data_ref.concat();
        let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
            0 => None,
            _ => Some(validity.into()),
        };

        let arrow_dtype = arrow2::datatypes::DataType::FixedSizeList(
            Box::new(arrow2::datatypes::Field::new(
                "data",
                arrow2::datatypes::DataType::UInt8,
                true,
            )),
            list_size,
        );
        let arrow_array = Box::new(arrow2::array::FixedSizeListArray::new(
            arrow_dtype.clone(),
            Box::new(arrow2::array::PrimitiveArray::from_vec(data)),
            validity,
        ));
        let physical_array = FixedSizeListArray::new(
            Field::new(name, (&arrow_dtype).into()).into(),
            arrow_array.boxed(),
        )?;
        let logical_dtype = DataType::FixedShapeImage(*image_mode, height, width);
        Ok(Self::new(Field::new(name, logical_dtype), physical_array))
    }

    pub fn encode(&self, image_format: ImageFormat) -> DaftResult<BinaryArray> {
        encode_images(self, image_format)
    }

    pub fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let result = resize_images(self, w, h);
        match self.logical_type() {
            DataType::FixedShapeImage(mode, _, _) => Self::from_daft_image_buffers(self.name(), result.as_slice(), mode, h, w),
            dt => panic!("FixedShapeImageArray should always have DataType::FixedShapeImage() as it's dtype, but got {}", dt),
        }
    }

    pub fn crop(&self, bboxes: &FixedSizeListArray) -> DaftResult<ImageArray> {
        let mut bboxes_iterator: Box<dyn Iterator<Item = Option<BBox>>> = if bboxes.len() == 1 {
            Box::new(std::iter::repeat(
                bboxes
                    .as_arrow()
                    .get(0)
                    .map(|bbox| BBox::from_u32_arrow_array(bbox)),
            ))
        } else {
            Box::new(
                bboxes
                    .as_arrow()
                    .iter()
                    .map(|bbox| bbox.map(|bbox| BBox::from_u32_arrow_array(bbox))),
            )
        };
        let result = crop_images(self, &mut bboxes_iterator);
        ImageArray::from_daft_image_buffers(self.name(), result.as_slice(), &Some(self.mode()))
    }
}

impl AsImageObj for FixedShapeImageArray {
    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<DaftImageBuffer<'a>> {
        assert!(idx < self.len());
        if !self.physical.is_valid(idx) {
            return None;
        }

        match self.logical_type() {
            DataType::FixedShapeImage(mode, height, width) => {
                let arrow_array = self.as_arrow().values().as_any().downcast_ref::<arrow2::array::UInt8Array>().unwrap();
                let num_channels = mode.num_channels();
                let size = height * width * num_channels as u32;
                let start = idx * size as usize;
                let end = (idx + 1) * size as usize;
                let slice_data = Cow::Borrowed(&arrow_array.values().as_slice()[start..end] as &'a [u8]);
                let result = match mode {
                    ImageMode::L => {
                        DaftImageBuffer::<'a>::L(ImageBuffer::from_raw(*width, *height, slice_data).unwrap())
                    }
                    ImageMode::LA => {
                        DaftImageBuffer::<'a>::LA(ImageBuffer::from_raw(*width, *height, slice_data).unwrap())
                    }
                    ImageMode::RGB => {
                        DaftImageBuffer::<'a>::RGB(ImageBuffer::from_raw(*width, *height, slice_data).unwrap())
                    }
                    ImageMode::RGBA => {
                        DaftImageBuffer::<'a>::RGBA(ImageBuffer::from_raw(*width, *height, slice_data).unwrap())
                    }
                    _ => unimplemented!("{mode} is currently not implemented!"),
                };

                assert_eq!(result.height(), *height);
                assert_eq!(result.width(), *width);
                Some(result)
            }
            dt => panic!("FixedShapeImageArray should always have DataType::FixedShapeImage() as it's dtype, but got {}", dt),
        }
    }
}

impl<'a, T> IntoIterator for &'a LogicalArray<T>
where
    T: DaftImageryType,
    LogicalArray<T>: AsImageObj,
{
    type Item = Option<DaftImageBuffer<'a>>;
    type IntoIter = ImageBufferIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        ImageBufferIter::new(self)
    }
}

impl BinaryArray {
    pub fn image_decode(&self) -> DaftResult<ImageArray> {
        let arrow_array = self
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::BinaryArray<i64>>()
            .unwrap();
        let mut img_bufs = Vec::<Option<DaftImageBuffer>>::with_capacity(arrow_array.len());
        let mut cached_dtype: Option<DataType> = None;
        // Load images from binary buffers.
        // Confirm that all images have the same value dtype.
        for row in arrow_array.iter() {
            let img_buf = row.map(DaftImageBuffer::decode).transpose()?;
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
        // Fall back to UInt8 dtype if series is all nulls.
        let cached_dtype = cached_dtype.unwrap_or(DataType::UInt8);
        match cached_dtype {
            DataType::UInt8 => Ok(ImageArray::from_daft_image_buffers(self.name(), img_bufs.as_slice(), &None)?),
            _ => unimplemented!("Decoding images of dtype {cached_dtype:?} is not supported, only uint8 images are supported."),
        }
    }
}

fn encode_images<'a, T>(
    images: &'a LogicalArray<T>,
    image_format: ImageFormat,
) -> DaftResult<BinaryArray>
where
    T: DaftImageryType,
    LogicalArray<T>: AsImageObj,
    &'a LogicalArray<T>:
        IntoIterator<Item = Option<DaftImageBuffer<'a>>, IntoIter = ImageBufferIter<'a, T>>,
{
    let arrow_array = match image_format {
        ImageFormat::TIFF => {
            // NOTE: A single writer/buffer can't be used for TIFF files because the encoder will overwrite the
            // IFD offset for the first image instead of writing it for all subsequent images, producing corrupted
            // TIFF files. We work around this by writing out a new buffer for each image.
            // TODO(Clark): Fix this in the tiff crate.
            let values = images
                .into_iter()
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
                                    "Encoding image into file format {} failed: {}",
                                    image_format, e
                                ))
                            })?
                            .into_inner())
                    })
                    .transpose()
                })
                .collect::<DaftResult<Vec<_>>>()?;
            arrow2::array::BinaryArray::<i64>::from_iter(values.into_iter())
        }
        _ => {
            let mut offsets = Vec::with_capacity(images.len() + 1);
            offsets.push(0i64);
            let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(images.len());
            let buf = Vec::new();
            let mut writer: CountingWriter<std::io::BufWriter<_>> =
                std::io::BufWriter::new(std::io::Cursor::new(buf)).into();
            images
                .into_iter()
                .map(|img| {
                    match img {
                        Some(img) => {
                            img.encode(image_format, &mut writer)?;
                            offsets.push(writer.count() as i64);
                            validity.push(true);
                        }
                        None => {
                            offsets.push(*offsets.last().unwrap());
                            validity.push(false);
                        }
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
                        "Encoding image into file format {} failed: {}",
                        image_format, e
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
        }
    };
    BinaryArray::new(
        Field::new(images.name(), arrow_array.data_type().into()).into(),
        arrow_array.boxed(),
    )
}

fn resize_images<'a, T>(images: &'a LogicalArray<T>, w: u32, h: u32) -> Vec<Option<DaftImageBuffer>>
where
    T: DaftImageryType,
    LogicalArray<T>: AsImageObj,
    &'a LogicalArray<T>:
        IntoIterator<Item = Option<DaftImageBuffer<'a>>, IntoIter = ImageBufferIter<'a, T>>,
{
    images
        .into_iter()
        .map(|img| img.map(|img| img.resize(w, h)))
        .collect::<Vec<_>>()
}

fn crop_images<'a, T>(
    images: &'a LogicalArray<T>,
    bboxes: &mut dyn Iterator<Item = Option<BBox>>,
) -> Vec<Option<DaftImageBuffer<'a>>>
where
    T: DaftImageryType,
    LogicalArray<T>: AsImageObj,
    &'a LogicalArray<T>:
        IntoIterator<Item = Option<DaftImageBuffer<'a>>, IntoIter = ImageBufferIter<'a, T>>,
{
    images
        .into_iter()
        .zip(bboxes)
        .map(|(img, bbox)| match (img, bbox) {
            (None, _) | (_, None) => None,
            (Some(img), Some(bbox)) => Some(img.crop(&bbox)),
        })
        .collect::<Vec<_>>()
}
