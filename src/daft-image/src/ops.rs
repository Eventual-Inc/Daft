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
        let buffers = ImageBufferIter::new(self).map(|img| img.map(|img| img.into_mode(mode)));
        image_array_from_img_buffers(self.name(), buffers, Some(mode))
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
        daft_arrow::array::BinaryArray::<i64>::from_iter(values)
    } else {
        let mut offsets = Vec::with_capacity(images.len() + 1);
        offsets.push(0i64);
        let mut validity = daft_arrow::buffer::NullBufferBuilder::new(images.len());
        let buf = Vec::new();
        let mut writer: CountingWriter<std::io::BufWriter<_>> =
            std::io::BufWriter::new(std::io::Cursor::new(buf)).into();
        ImageBufferIter::new(images)
            .map(|img| {
                if let Some(img) = img {
                    img.encode(image_format, &mut writer)?;
                    offsets.push(writer.count() as i64);
                    validity.append_non_null();
                } else {
                    offsets.push(*offsets.last().unwrap());
                    validity.append_null();
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
        let encoded_data: daft_arrow::buffer::Buffer<u8> = values.into();
        let offsets_buffer = daft_arrow::offset::OffsetsBuffer::try_from(offsets)?;
        let validity = validity.finish();
        daft_arrow::array::BinaryArray::<i64>::new(
            daft_arrow::datatypes::DataType::LargeBinary,
            offsets_buffer,
            encoded_data,
            daft_arrow::buffer::wrap_null_buffer(validity),
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
