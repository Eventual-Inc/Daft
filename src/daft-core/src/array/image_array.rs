use std::vec;

use common_error::DaftResult;

use crate::{
    array::prelude::*,
    datatypes::prelude::*,
    series::{IntoSeries, Series},
};

pub struct ImageArraySidecarData {
    pub channels: Vec<u16>,
    pub heights: Vec<u32>,
    pub widths: Vec<u32>,
    pub modes: Vec<u8>,
    pub validity: Option<daft_arrow::buffer::NullBuffer>,
}

impl ImageArray {
    pub const IMAGE_DATA_IDX: usize = 0;
    pub const IMAGE_CHANNEL_IDX: usize = 1;
    pub const IMAGE_HEIGHT_IDX: usize = 2;
    pub const IMAGE_WIDTH_IDX: usize = 3;
    pub const IMAGE_MODE_IDX: usize = 4;

    pub fn image_mode(&self) -> Option<ImageMode> {
        match self.data_type() {
            DataType::Image(mode) => *mode,
            _ => panic!("Expected dtype to be Image"),
        }
    }

    pub fn data_array(&self) -> &ListArray {
        let array = self.physical.children.get(Self::IMAGE_DATA_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn channel_array(&self) -> &daft_arrow::array::UInt16Array {
        self.channels().as_arrow2()
    }

    pub fn height_array(&self) -> &daft_arrow::array::UInt32Array {
        self.heights().as_arrow2()
    }

    pub fn width_array(&self) -> &daft_arrow::array::UInt32Array {
        self.widths().as_arrow2()
    }

    pub fn mode_array(&self) -> &daft_arrow::array::UInt8Array {
        self.modes().as_arrow2()
    }

    pub fn channels(&self) -> &DataArray<UInt16Type> {
        let array = self.physical.children.get(Self::IMAGE_CHANNEL_IDX).unwrap();
        array.u16().unwrap()
    }

    pub fn heights(&self) -> &DataArray<UInt32Type> {
        let array = self.physical.children.get(Self::IMAGE_HEIGHT_IDX).unwrap();
        array.u32().unwrap()
    }

    pub fn widths(&self) -> &DataArray<UInt32Type> {
        let array = self.physical.children.get(Self::IMAGE_WIDTH_IDX).unwrap();
        array.u32().unwrap()
    }

    pub fn modes(&self) -> &DataArray<UInt8Type> {
        let array = self.physical.children.get(Self::IMAGE_MODE_IDX).unwrap();
        array.u8().unwrap()
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
                    daft_arrow::array::UInt16Array::from_vec(sidecar_data.channels).with_validity(
                        daft_arrow::buffer::wrap_null_buffer(sidecar_data.validity.clone()),
                    ),
                ),
            ))
            .into_series(),
            UInt32Array::from((
                "height",
                Box::new(
                    daft_arrow::array::UInt32Array::from_vec(sidecar_data.heights).with_validity(
                        daft_arrow::buffer::wrap_null_buffer(sidecar_data.validity.clone()),
                    ),
                ),
            ))
            .into_series(),
            UInt32Array::from((
                "width",
                Box::new(
                    daft_arrow::array::UInt32Array::from_vec(sidecar_data.widths).with_validity(
                        daft_arrow::buffer::wrap_null_buffer(sidecar_data.validity.clone()),
                    ),
                ),
            ))
            .into_series(),
            UInt8Array::from((
                "mode",
                Box::new(
                    daft_arrow::array::UInt8Array::from_vec(sidecar_data.modes).with_validity(
                        daft_arrow::buffer::wrap_null_buffer(sidecar_data.validity.clone()),
                    ),
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

    pub fn from_vecs<T: daft_arrow::types::NativeType>(
        name: &str,
        data_type: DataType,
        data: Vec<T>,
        offsets: Vec<i64>,
        sidecar_data: ImageArraySidecarData,
    ) -> DaftResult<Self> {
        if data.is_empty() {
            return Ok(ImageArray::full_null(name, &data_type, offsets.len() - 1));
        }
        let offsets = daft_arrow::offset::OffsetsBuffer::try_from(offsets)?;
        let arrow_dtype: daft_arrow::datatypes::DataType = T::PRIMITIVE.into();
        if let DataType::Image(Some(mode)) = &data_type {
            assert!(
                !(mode.get_dtype().to_arrow()? != arrow_dtype),
                "Inner value dtype of provided dtype {data_type:?} is inconsistent with inferred value dtype {arrow_dtype:?}"
            );
        }
        let data_array = ListArray::new(
            Field::new("data", DataType::List(Box::new((&arrow_dtype).into()))),
            Series::try_from((
                "data",
                Box::new(daft_arrow::array::PrimitiveArray::from_vec(data))
                    as Box<dyn daft_arrow::array::Array>,
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
