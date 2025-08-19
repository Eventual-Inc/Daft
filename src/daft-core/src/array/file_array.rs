use std::sync::Arc;

use arrow2::Either;
use common_io_config::IOConfig;
use daft_schema::field::Field;

use crate::array::prelude::*;

#[derive(Debug)]
pub struct FileArray {
    pub field: Arc<Field>,
    pub data: Either<Utf8Array, BinaryArray>,
    pub io_configs: Vec<Option<IOConfig>>,
}

impl FileArray {
    pub fn new_from_data_array(name: &str, data: &BinaryArray) -> Self {
        todo!()
    }

    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        todo!()
    }

    pub fn data_array(&self) -> BinaryArray {
        todo!()
        // self.physical.get("data").unwrap().binary().unwrap().clone()
    }

    // pub fn from_list_array(
    //     name: &str,
    //     data_type: DataType,
    //     data_array: ListArray,
    //     sidecar_data: ImageArraySidecarData,
    // ) -> DaftResult<Self> {
    //     let values: Vec<Series> = vec![
    //         data_array.into_series().rename("data"),
    //         UInt16Array::from((
    //             "channel",
    //             Box::new(
    //                 arrow2::array::UInt16Array::from_vec(sidecar_data.channels)
    //                     .with_validity(sidecar_data.validity.clone()),
    //             ),
    //         ))
    //         .into_series(),
    //         UInt32Array::from((
    //             "height",
    //             Box::new(
    //                 arrow2::array::UInt32Array::from_vec(sidecar_data.heights)
    //                     .with_validity(sidecar_data.validity.clone()),
    //             ),
    //         ))
    //         .into_series(),
    //         UInt32Array::from((
    //             "width",
    //             Box::new(
    //                 arrow2::array::UInt32Array::from_vec(sidecar_data.widths)
    //                     .with_validity(sidecar_data.validity.clone()),
    //             ),
    //         ))
    //         .into_series(),
    //         UInt8Array::from((
    //             "mode",
    //             Box::new(
    //                 arrow2::array::UInt8Array::from_vec(sidecar_data.modes)
    //                     .with_validity(sidecar_data.validity.clone()),
    //             ),
    //         ))
    //         .into_series(),
    //     ];
    //     let physical_type = data_type.to_physical();
    //     let struct_array = StructArray::new(
    //         Field::new(name, physical_type),
    //         values,
    //         sidecar_data.validity,
    //     );
    //     Ok(ImageArray::new(Field::new(name, data_type), struct_array))
    // }

    // pub fn from_vecs<T: arrow2::types::NativeType>(
    //     name: &str,
    //     data_type: DataType,
    //     data: Vec<T>,
    //     offsets: Vec<i64>,
    //     sidecar_data: ImageArraySidecarData,
    // ) -> DaftResult<Self> {
    //     if data.is_empty() {
    //         return Ok(ImageArray::full_null(name, &data_type, offsets.len() - 1));
    //     }
    //     let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
    //     let arrow_dtype: arrow2::datatypes::DataType = T::PRIMITIVE.into();
    //     if let DataType::Image(Some(mode)) = &data_type {
    //         assert!(!(mode.get_dtype().to_arrow()? != arrow_dtype), "Inner value dtype of provided dtype {data_type:?} is inconsistent with inferred value dtype {arrow_dtype:?}");
    //     }
    //     let data_array = ListArray::new(
    //         Field::new("data", DataType::List(Box::new((&arrow_dtype).into()))),
    //         Series::try_from((
    //             "data",
    //             Box::new(arrow2::array::PrimitiveArray::from_vec(data))
    //                 as Box<dyn arrow2::array::Array>,
    //         ))?,
    //         offsets,
    //         sidecar_data.validity.clone(),
    //     );

    //     Self::from_list_array(name, data_type, data_array, sidecar_data)
    // }
}
