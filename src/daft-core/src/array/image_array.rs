use std::io::{Seek, SeekFrom, Write};
use std::vec;

use common_error::DaftResult;

use crate::array::prelude::*;
use crate::datatypes::prelude::*;

use crate::series::{IntoSeries, Series};

#[derive(Clone)]
pub struct BBox(pub u32, pub u32, pub u32, pub u32);

impl BBox {
    pub fn from_u32_arrow_array(arr: &dyn arrow2::array::Array) -> Self {
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

type IOResult<T = (), E = std::io::Error> = std::result::Result<T, E>;

/// A wrapper of a writer that tracks the number of bytes successfully written.
pub struct CountingWriter<W> {
    inner: W,
    count: u64,
}

impl<W> CountingWriter<W> {
    /// The number of bytes successful written so far.
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

pub struct ImageArraySidecarData {
    pub channels: Vec<u16>,
    pub heights: Vec<u32>,
    pub widths: Vec<u32>,
    pub modes: Vec<u8>,
    pub validity: Option<arrow2::bitmap::Bitmap>,
}

impl ImageArray {
    pub fn image_mode(&self) -> &Option<ImageMode> {
        match self.data_type() {
            DataType::Image(mode) => mode,
            _ => panic!("Expected dtype to be Image"),
        }
    }

    pub fn data_array(&self) -> &ListArray {
        const IMAGE_DATA_IDX: usize = 0;
        let array = self.physical.children.get(IMAGE_DATA_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn channel_array(&self) -> &arrow2::array::UInt16Array {
        const IMAGE_CHANNEL_IDX: usize = 1;
        let array = self.physical.children.get(IMAGE_CHANNEL_IDX).unwrap();
        array.u16().unwrap().as_arrow()
    }

    pub fn height_array(&self) -> &arrow2::array::UInt32Array {
        const IMAGE_HEIGHT_IDX: usize = 2;
        let array = self.physical.children.get(IMAGE_HEIGHT_IDX).unwrap();
        array.u32().unwrap().as_arrow()
    }

    pub fn width_array(&self) -> &arrow2::array::UInt32Array {
        const IMAGE_WIDTH_IDX: usize = 3;
        let array = self.physical.children.get(IMAGE_WIDTH_IDX).unwrap();
        array.u32().unwrap().as_arrow()
    }

    pub fn mode_array(&self) -> &arrow2::array::UInt8Array {
        const IMAGE_MODE_IDX: usize = 4;
        let array = self.physical.children.get(IMAGE_MODE_IDX).unwrap();
        array.u8().unwrap().as_arrow()
    }

    pub fn from_list_array(
        name: &str,
        data_type: DataType,
        data_array: ListArray,
        sidecar_data: ImageArraySidecarData,
    ) -> DaftResult<Self> {
        let values: Vec<Series> = vec![
            data_array.into_series().rename("data"),
            UInt16Array::from((
                "channel",
                Box::new(
                    arrow2::array::UInt16Array::from_vec(sidecar_data.channels)
                        .with_validity(sidecar_data.validity.clone()),
                ),
            ))
            .into_series(),
            UInt32Array::from((
                "height",
                Box::new(
                    arrow2::array::UInt32Array::from_vec(sidecar_data.heights)
                        .with_validity(sidecar_data.validity.clone()),
                ),
            ))
            .into_series(),
            UInt32Array::from((
                "width",
                Box::new(
                    arrow2::array::UInt32Array::from_vec(sidecar_data.widths)
                        .with_validity(sidecar_data.validity.clone()),
                ),
            ))
            .into_series(),
            UInt8Array::from((
                "mode",
                Box::new(
                    arrow2::array::UInt8Array::from_vec(sidecar_data.modes)
                        .with_validity(sidecar_data.validity.clone()),
                ),
            ))
            .into_series(),
        ];
        let physical_type = data_type.to_physical();
        let struct_array = StructArray::new(
            Field::new(name, physical_type),
            values,
            sidecar_data.validity,
        );
        Ok(ImageArray::new(Field::new(name, data_type), struct_array))
    }

    pub fn from_vecs<T: arrow2::types::NativeType>(
        name: &str,
        data_type: DataType,
        data: Vec<T>,
        offsets: Vec<i64>,
        sidecar_data: ImageArraySidecarData,
    ) -> DaftResult<Self> {
        if data.is_empty() {
            return Ok(ImageArray::full_null(name, &data_type, offsets.len() - 1));
        }
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let arrow_dtype: arrow2::datatypes::DataType = T::PRIMITIVE.into();
        if let DataType::Image(Some(mode)) = &data_type {
            if mode.get_dtype().to_arrow()? != arrow_dtype {
                panic!("Inner value dtype of provided dtype {data_type:?} is inconsistent with inferred value dtype {arrow_dtype:?}");
            }
        }
        let data_array = ListArray::new(
            Field::new("data", DataType::List(Box::new((&arrow_dtype).into()))),
            Series::try_from((
                "data",
                Box::new(arrow2::array::PrimitiveArray::from_vec(data))
                    as Box<dyn arrow2::array::Array>,
            ))?,
            offsets,
            sidecar_data.validity.clone(),
        );

        Self::from_list_array(name, data_type, data_array, sidecar_data)
    }
}

impl FixedShapeImageArray {
    pub fn image_mode(&self) -> &ImageMode {
        match self.data_type() {
            DataType::FixedShapeImage(mode, _, _) => mode,
            other => panic!("Expected dtype to be Image, got {other:?}"),
        }
    }
}
