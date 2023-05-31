use std::borrow::Cow;
use std::vec;

use image::ImageBuffer;

use crate::datatypes::logical::ImageArray;
use crate::datatypes::{Field, ImageMode};
use crate::error::DaftResult;
use image::{Luma, LumaA, Rgb, Rgba};

use super::as_arrow::AsArrow;
use num_traits::FromPrimitive;

use std::ops::Deref;

#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug)]
enum DaftImageBuffer<'a> {
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
    pub fn height(&self) -> u32 {
        with_method_on_image_buffer!(self, height)
    }

    pub fn width(&self) -> u32 {
        with_method_on_image_buffer!(self, width)
    }

    pub fn channels(&self) -> u16 {
        use DaftImageBuffer::*;
        match self {
            L(..) | L16(..) => 1,
            LA(..) | LA16(..) => 2,
            RGB(..) | RGB16(..) | RGB32F(..) => 3,
            RGBA(..) | RGBA16(..) | RGBA32F(..) => 4,
        }
    }

    pub fn get_as_u8(&'a self) -> &'a [u8] {
        use DaftImageBuffer::*;
        match self {
            L(img) => img.as_raw(),
            LA(img) => img.as_raw(),
            RGB(img) => img.as_raw(),
            RGBA(img) => img.as_raw(),
            _ => unimplemented!("unimplemented {self:?}"),
        }
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
            _ => unimplemented!("mode not implemented"),
        }
    }
}

fn collate_daft_image_buffers(
    name: &str,
    inputs: &[Option<DaftImageBuffer<'_>>],
) -> DaftResult<ImageArray> {
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
    let mut is_valid = Vec::with_capacity(inputs.len());
    offsets.push(0i64);

    for ib in inputs {
        if let Some(ib) = ib {
            heights.push(ib.height());
            widths.push(ib.width());
            channels.push(ib.channels());

            let buffer = ib.get_as_u8();
            data_ref.push(buffer);
            offsets.push(buffer.len() as i64 + offsets.last().unwrap());
            modes.push(ib.mode() as u8);
            is_valid.push(true);
        } else {
            heights.push(0u32);
            widths.push(0u32);
            channels.push(0u16);
            offsets.push(*offsets.last().unwrap());
            modes.push(0);
            is_valid.push(false);
        }
    }

    let collected_data = data_ref.concat();
    let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
    let data_type =
        crate::datatypes::DataType::Image(Box::new(crate::datatypes::DataType::UInt8), None);

    let validity = arrow2::bitmap::Bitmap::from(is_valid);

    let list_datatype = arrow2::datatypes::DataType::LargeList(Box::new(
        arrow2::datatypes::Field::new("data", arrow2::datatypes::DataType::UInt8, true),
    ));
    let data_array = Box::new(arrow2::array::ListArray::<i64>::new(
        list_datatype,
        offsets,
        Box::new(arrow2::array::UInt8Array::from_vec(collected_data)),
        Some(validity.clone()),
    ));

    let values: Vec<Box<dyn arrow2::array::Array>> = vec![
        data_array,
        Box::new(
            arrow2::array::UInt16Array::from_vec(channels).with_validity(Some(validity.clone())),
        ),
        Box::new(
            arrow2::array::UInt32Array::from_vec(heights).with_validity(Some(validity.clone())),
        ),
        Box::new(
            arrow2::array::UInt32Array::from_vec(widths).with_validity(Some(validity.clone())),
        ),
        Box::new(arrow2::array::UInt8Array::from_vec(modes).with_validity(Some(validity.clone()))),
    ];
    let physical_type = data_type.to_physical();
    let struct_array = Box::new(arrow2::array::StructArray::new(
        physical_type.to_arrow()?,
        values,
        Some(validity),
    ));

    let daft_struct_array =
        crate::datatypes::StructArray::new(Field::new(name, physical_type).into(), struct_array)?;
    Ok(ImageArray::new(
        Field::new(name, data_type),
        daft_struct_array,
    ))
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

impl ImageArray {
    fn data_array(&self) -> &arrow2::array::ListArray<i64> {
        let p = self.physical.as_arrow();
        const IMAGE_DATA_IDX: usize = 0;
        let array = p.values().get(IMAGE_DATA_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn channel_array(&self) -> &arrow2::array::UInt16Array {
        let p = self.physical.as_arrow();
        const IMAGE_CHANNEL_IDX: usize = 1;
        let array = p.values().get(IMAGE_CHANNEL_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn height_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_HEIGHT_IDX: usize = 2;
        let array = p.values().get(IMAGE_HEIGHT_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn width_array(&self) -> &arrow2::array::UInt32Array {
        let p = self.physical.as_arrow();
        const IMAGE_WIDTH_IDX: usize = 3;
        let array = p.values().get(IMAGE_WIDTH_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    fn mode_array(&self) -> &arrow2::array::UInt8Array {
        let p = self.physical.as_arrow();
        const IMAGE_MODE_IDX: usize = 4;
        let array = p.values().get(IMAGE_MODE_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

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
        Some(match m {
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
        })
    }

    pub fn resize(&self, w: u32, h: u32) -> DaftResult<Self> {
        let result = (0..self.len())
            .map(|i| self.as_image_obj(i))
            .map(|img| img.map(|img| img.resize(w, h)))
            .collect::<Vec<_>>();

        collate_daft_image_buffers(self.name(), result.as_slice())
    }
}
