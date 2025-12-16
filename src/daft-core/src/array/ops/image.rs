use std::{borrow::Cow, sync::Arc};

use common_error::DaftResult;
use common_image::CowImage;
use num_traits::FromPrimitive;

use crate::{array::image_array::ImageArraySidecarData, prelude::*};

#[allow(clippy::len_without_is_empty)]
pub trait AsImageObj {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn as_image_obj(&self, idx: usize) -> Option<CowImage<'_>>;
}

impl AsImageObj for ImageArray {
    fn len(&self) -> usize {
        ImageArray::len(self)
    }

    fn name(&self) -> &str {
        ImageArray::name(self)
    }

    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<CowImage<'a>> {
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
            .downcast_ref::<daft_arrow::array::UInt8Array>()
            .unwrap();
        let slice_data = Cow::Borrowed(&values.values().as_slice()[start..end] as &'a [u8]);

        let c = ca.value(idx);
        let h = ha.value(idx);
        let w = wa.value(idx);
        let m: ImageMode = ImageMode::from_u8(ma.value(idx)).unwrap();
        assert_eq!(m.num_channels(), c);
        let result = CowImage::from_raw(&m, w, h, slice_data);

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

    fn as_image_obj<'a>(&'a self, idx: usize) -> Option<CowImage<'a>> {
        assert!(idx < self.len());
        if !self.physical.is_valid(idx) {
            return None;
        }

        match self.data_type() {
            DataType::FixedShapeImage(mode, height, width) => {
                let arrow_array = self
                    .physical
                    .flat_child
                    .downcast::<UInt8Array>()
                    .unwrap()
                    .as_arrow2();
                let num_channels = mode.num_channels();
                let size = height * width * u32::from(num_channels);
                let start = idx * size as usize;
                let end = (idx + 1) * size as usize;
                let slice_data =
                    Cow::Borrowed(&arrow_array.values().as_slice()[start..end] as &'a [u8]);
                let result = CowImage::from_raw(mode, *width, *height, slice_data);

                assert_eq!(result.height(), *height);
                assert_eq!(result.width(), *width);
                Some(result)
            }
            dt => panic!(
                "FixedShapeImageArray should always have DataType::FixedShapeImage() as it's dtype, but got {dt}"
            ),
        }
    }
}

pub fn image_array_from_img_buffers<'a, I>(
    name: &str,
    inputs: I,
    image_mode: Option<ImageMode>,
) -> DaftResult<ImageArray>
where
    I: ExactSizeIterator<Item = Option<CowImage<'a>>>,
{
    use CowImage::{L, LA, RGB, RGBA};

    let mut data = Vec::with_capacity(inputs.len());
    let mut heights = Vec::with_capacity(inputs.len());
    let mut channels = Vec::with_capacity(inputs.len());
    let mut modes = Vec::with_capacity(inputs.len());
    let mut widths = Vec::with_capacity(inputs.len());
    let mut offsets = Vec::with_capacity(inputs.len() + 1);
    offsets.push(0i64);
    let mut validity = daft_arrow::buffer::NullBufferBuilder::new(inputs.len());

    for ib in inputs {
        validity.append(ib.is_some());
        match ib {
            Some(ib) => {
                assert!(matches!(&ib, L(..) | LA(..) | RGB(..) | RGBA(..)));
                let height = ib.height();
                let width = ib.width();
                let mode = ib.mode();
                let buffer = ib.as_u8_slice();
                heights.push(height);
                widths.push(width);
                modes.push(mode as u8);
                channels.push(mode.num_channels());
                data.extend_from_slice(buffer);
                offsets.push(offsets.last().unwrap() + buffer.len() as i64);
            }
            None => {
                heights.push(0u32);
                widths.push(0u32);
                modes.push(ImageMode::L as u8);
                channels.push(ImageMode::L.num_channels());
                data.extend_from_slice(&[] as &[u8]);
                offsets.push(*offsets.last().unwrap());
            }
        }
    }

    let validity = validity.finish();
    ImageArray::from_vecs(
        name,
        DataType::Image(image_mode),
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

pub fn fixed_image_array_from_img_buffers(
    name: &str,
    inputs: &[Option<CowImage<'_>>],
    image_mode: &ImageMode,
    height: u32,
    width: u32,
) -> DaftResult<FixedShapeImageArray> {
    use CowImage::{L, LA, RGB, RGBA};
    let is_all_u8 = inputs
        .iter()
        .filter_map(|b| b.as_ref())
        .all(|b| matches!(b, L(..) | LA(..) | RGB(..) | RGBA(..)));
    assert!(is_all_u8);

    let num_channels = image_mode.num_channels();
    let mut data_ref = Vec::with_capacity(inputs.len());
    let mut validity = daft_arrow::buffer::NullBufferBuilder::new(inputs.len());
    let list_size = (height * width * u32::from(num_channels)) as usize;
    let null_list = vec![0u8; list_size];
    for ib in inputs {
        validity.append(ib.is_some());
        let buffer = match ib {
            Some(ib) => ib.as_u8_slice(),
            None => null_list.as_slice(),
        };
        data_ref.push(buffer);
    }
    let data = data_ref.concat();
    let validity = validity.finish();

    let arrow_dtype = daft_arrow::datatypes::DataType::FixedSizeList(
        Box::new(daft_arrow::datatypes::Field::new(
            "data",
            daft_arrow::datatypes::DataType::UInt8,
            true,
        )),
        list_size,
    );
    let arrow_array = Box::new(daft_arrow::array::FixedSizeListArray::new(
        arrow_dtype.clone(),
        Box::new(daft_arrow::array::PrimitiveArray::from_vec(data)),
        daft_arrow::buffer::wrap_null_buffer(validity),
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
