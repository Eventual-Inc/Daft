use std::{borrow::Cow, sync::Arc};

use base64::Engine;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::{
        image_array::{BBox, ImageArraySidecarData},
        prelude::*,
    },
    datatypes::prelude::*,
    prelude::ImageArray,
};
use num_traits::FromPrimitive;

use crate::{iters::ImageBufferIter, CountingWriter, DaftImageBuffer};

#[allow(clippy::len_without_is_empty)]
pub trait AsImageObj {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn as_image_obj(&self, idx: usize) -> Option<DaftImageBuffer<'_>>;
}

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
}

pub(crate) fn image_array_from_img_buffers(
    name: &str,
    inputs: &[Option<DaftImageBuffer<'_>>],
    image_mode: &Option<ImageMode>,
) -> DaftResult<ImageArray> {
    use DaftImageBuffer::{L, LA, RGB, RGBA};
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
    ImageArray::from_vecs(
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

pub(crate) fn fixed_image_array_from_img_buffers(
    name: &str,
    inputs: &[Option<DaftImageBuffer<'_>>],
    image_mode: &ImageMode,
    height: u32,
    width: u32,
) -> DaftResult<FixedShapeImageArray> {
    use DaftImageBuffer::{L, LA, RGB, RGBA};
    let is_all_u8 = inputs
        .iter()
        .filter_map(|b| b.as_ref())
        .all(|b| matches!(b, L(..) | LA(..) | RGB(..) | RGBA(..)));
    assert!(is_all_u8);

    let num_channels = image_mode.num_channels();
    let mut data_ref = Vec::with_capacity(inputs.len());
    let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(inputs.len());
    let list_size = (height * width * u32::from(num_channels)) as usize;
    let null_list = vec![0u8; list_size];
    for ib in inputs {
        validity.push(ib.is_some());
        let buffer = match ib {
            Some(ib) => ib.as_u8_slice(),
            None => null_list.as_slice(),
        };
        data_ref.push(buffer);
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
    let physical_array = FixedSizeListArray::from_arrow(
        Arc::new(Field::new(name, (&arrow_dtype).into())),
        arrow_array,
    )?;
    let logical_dtype = DataType::FixedShapeImage(*image_mode, height, width);
    Ok(FixedShapeImageArray::new(
        Field::new(name, logical_dtype),
        physical_array,
    ))
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
        let buffers: Vec<Option<DaftImageBuffer>> = ImageBufferIter::new(self)
            .map(|img| img.map(|img| img.into_mode(mode)))
            .collect();
        image_array_from_img_buffers(self.name(), &buffers, &Some(mode))
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

        image_array_from_img_buffers(self.name(), result.as_slice(), &Some(*self.image_mode()))
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
        let buffers: Vec<Option<DaftImageBuffer>> = ImageBufferIter::new(self)
            .map(|img| img.map(|img| img.into_mode(mode)))
            .collect();

        let (height, width) = match self.data_type() {
            DataType::FixedShapeImage(_, h, w) => (h, w),
            _ => unreachable!("self should always be a FixedShapeImage"),
        };
        fixed_image_array_from_img_buffers(self.name(), &buffers, &mode, *height, *width)
    }
}

impl AsImageObj for ImageArray {
    fn len(&self) -> usize {
        ImageArray::len(self)
    }

    fn name(&self) -> &str {
        ImageArray::name(self)
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
            .flat_child
            .u8()
            .unwrap()
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::UInt8Array>()
            .unwrap();
        let slice_data = Cow::Borrowed(&values.values().as_slice()[start..end] as &'a [u8]);

        let c = ca.value(idx);
        let h = ha.value(idx);
        let w = wa.value(idx);
        let m: ImageMode = ImageMode::from_u8(ma.value(idx)).unwrap();
        assert_eq!(m.num_channels(), c);
        let result = DaftImageBuffer::from_raw(&m, w, h, slice_data);

        assert_eq!(result.height(), h);
        assert_eq!(result.width(), w);
        Some(result)
    }
}

impl AsImageObj for FixedShapeImageArray {
    fn len(&self) -> usize {
        FixedShapeImageArray::len(self)
    }

    fn name(&self) -> &str {
        FixedShapeImageArray::name(self)
    }

    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<DaftImageBuffer<'a>> {
        assert!(idx < self.len());
        if !self.physical.is_valid(idx) {
            return None;
        }

        match self.data_type() {
            DataType::FixedShapeImage(mode, height, width) => {
                let arrow_array = self.physical.flat_child.downcast::<UInt8Array>().unwrap().as_arrow();
                let num_channels = mode.num_channels();
                let size = height * width * u32::from(num_channels);
                let start = idx * size as usize;
                let end = (idx + 1) * size as usize;
                let slice_data = Cow::Borrowed(&arrow_array.values().as_slice()[start..end] as &'a [u8]);
                let result = DaftImageBuffer::from_raw(mode, *width, *height, slice_data);

                assert_eq!(result.height(), *height);
                assert_eq!(result.width(), *width);
                Some(result)
            }
            dt => panic!("FixedShapeImageArray should always have DataType::FixedShapeImage() as it's dtype, but got {dt}"),
        }
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

fn resize_images<Arr: AsImageObj>(images: &Arr, w: u32, h: u32) -> Vec<Option<DaftImageBuffer>> {
    ImageBufferIter::new(images)
        .map(|img| img.map(|img| img.resize(w, h)))
        .collect::<Vec<_>>()
}

fn crop_images<'a, Arr>(
    images: &'a Arr,
    bboxes: &mut dyn Iterator<Item = Option<BBox>>,
) -> Vec<Option<DaftImageBuffer<'a>>>
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
pub fn image_html_value(arr: &ImageArray, idx: usize) -> String {
    let maybe_image = arr.as_image_obj(idx);
    let str_val = arr.str_value(idx).unwrap();

    match maybe_image {
        None => "None".to_string(),
        Some(image) => {
            let thumb = image.fit_to(128, 128);
            let mut bytes: Vec<u8> = vec![];
            let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
            thumb.encode(ImageFormat::JPEG, &mut writer).unwrap();
            drop(writer);
            format!(
                "<img style=\"max-height:128px;width:auto\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                str_val,
            )
        }
    }
}

#[must_use]
pub fn fixed_image_html_value(arr: &FixedShapeImageArray, idx: usize) -> String {
    let maybe_image = arr.as_image_obj(idx);
    let str_val = arr.str_value(idx).unwrap();

    match maybe_image {
        None => "None".to_string(),
        Some(image) => {
            let thumb = image.fit_to(128, 128);
            let mut bytes: Vec<u8> = vec![];
            let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut bytes));
            thumb.encode(ImageFormat::JPEG, &mut writer).unwrap();
            drop(writer);
            format!(
                "<img style=\"max-height:128px;width:auto\" src=\"data:image/png;base64, {}\" alt=\"{}\" />",
                base64::engine::general_purpose::STANDARD.encode(&mut bytes),
                str_val,
            )
        }
    }
}
