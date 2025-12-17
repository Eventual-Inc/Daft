use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, AsArray},
    buffer::OffsetBuffer,
};
use common_error::{DaftError, DaftResult};
use daft_arrow::{array::Array as _, compute::cast::cast};

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        DaftDataType, DaftLogicalType, DaftPhysicalType, DataType, Field, FieldRef,
        logical::LogicalArray,
    },
    file::{DaftMediaType, FileType},
    prelude::*,
    series::Series,
};

/// Arrays that implement [`FromArrow`] can be instantiated from a Box<dyn daft_arrow::array::Array>
pub trait FromArrow
where
    Self: Sized,
{
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self>;
    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self>;
}

impl<T: DaftPhysicalType> FromArrow for DataArray<T> {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        Self::try_from((field, arrow_arr))
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        Self::new(field.into(), arrow_arr.into())
    }
}

impl FromArrow for FixedSizeListArray {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (
                DataType::FixedSizeList(daft_child_dtype, daft_size),
                daft_arrow::datatypes::DataType::FixedSizeList(_arrow_child_field, arrow_size),
            ) => {
                if daft_size != arrow_size {
                    return Err(DaftError::TypeError(format!(
                        "Attempting to create Daft FixedSizeListArray with element length {} from Arrow FixedSizeList array with element length {}",
                        daft_size, arrow_size
                    )));
                }

                let arrow_arr = arrow_arr
                    .as_ref()
                    .as_any()
                    .downcast_ref::<daft_arrow::array::FixedSizeListArray>()
                    .unwrap();
                let arrow_child_array = arrow_arr.values();
                let child_series = Series::from_arrow2(
                    Arc::new(Field::new("item", daft_child_dtype.as_ref().clone())),
                    arrow_child_array.clone(),
                )?;
                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned().map(Into::into),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft FixedSizeListArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field = field.into();

        match (&field.dtype, arrow_arr.data_type()) {
            (
                DataType::FixedSizeList(daft_child_dtype, daft_size),
                arrow::datatypes::DataType::FixedSizeList(_arrow_child_field, arrow_size),
            ) => {
                if *daft_size != (*arrow_size as usize) {
                    return Err(DaftError::TypeError(format!(
                        "Attempting to create Daft FixedSizeListArray with element length {} from Arrow FixedSizeList array with element length {}",
                        daft_size, arrow_size
                    )));
                }

                let arrow_arr = arrow_arr.as_fixed_size_list();

                let arrow_child_array = arrow_arr.values();
                let child_series = Series::from_arrow(
                    Arc::new(Field::new("item", daft_child_dtype.as_ref().clone())),
                    arrow_child_array.clone(),
                )?;
                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.nulls().cloned(),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft FixedSizeListArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }
    }
}

impl FromArrow for ListArray {
    fn from_arrow2(
        target_field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let target_dtype = &target_field.dtype;
        let arrow_dtype = arrow_arr.data_type();

        let result = match (target_dtype, arrow_dtype) {
            (
                DataType::List(daft_child_dtype),
                daft_arrow::datatypes::DataType::List(arrow_child_field),
            )
            | (
                DataType::List(daft_child_dtype),
                daft_arrow::datatypes::DataType::LargeList(arrow_child_field),
            ) => {
                // unifying lists
                let arrow_arr = cast(
                    &*arrow_arr,
                    &daft_arrow::datatypes::DataType::LargeList(arrow_child_field.clone()),
                    Default::default(),
                )?;

                let arrow_arr = arrow_arr
                    .as_any()
                    .downcast_ref::<daft_arrow::array::ListArray<i64>>() // list array with i64 offsets
                    .unwrap();

                let arrow_child_array = arrow_arr.values();
                let child_series = Series::from_arrow2(
                    Arc::new(Field::new("list", daft_child_dtype.as_ref().clone())),
                    arrow_child_array.clone(),
                )?;
                Ok(Self::new(
                    target_field.clone(),
                    child_series,
                    arrow_arr.offsets().clone(),
                    arrow_arr.validity().cloned().map(Into::into),
                ))
            }
            (DataType::List(daft_child_dtype), daft_arrow::datatypes::DataType::Map { .. }) => {
                Err(DaftError::TypeError(format!(
                    "Arrow Map type should be converted to Daft Map type, not List. Attempted to create Daft ListArray with type {daft_child_dtype} from Arrow Map type.",
                )))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft ListArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }?;

        Ok(result)
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field: FieldRef = field.into();
        let target_dtype = &field.dtype;

        let DataType::List(daft_child_dtype) = target_dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected List DataType, got {}",
                target_dtype
            )));
        };
        let list_arr = arrow_arr.as_list::<i64>();
        let arrow_child_array = list_arr.values();
        let child_field = Arc::new(Field::new("item", daft_child_dtype.as_ref().clone()));
        let child_series = Series::from_arrow(child_field, arrow_child_array.clone())?;
        let offsets: arrow::buffer::Buffer = list_arr.offsets().inner().clone().into_inner();

        let offsets =
            unsafe { daft_arrow::offset::OffsetsBuffer::<i64>::new_unchecked(offsets.into()) };
        let validity = list_arr.nulls().cloned();

        Ok(Self::new(field, child_series, offsets, validity))
    }
}

impl FromArrow for StructArray {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::Struct(fields), daft_arrow::datatypes::DataType::Struct(arrow_fields)) => {
                if fields.len() != arrow_fields.len() {
                    return Err(DaftError::ValueError(format!(
                        "Attempting to create Daft StructArray with {} fields from Arrow array with {} fields: {} vs {:?}",
                        fields.len(),
                        arrow_fields.len(),
                        &field.dtype,
                        arrow_arr.data_type()
                    )));
                }

                let arrow_arr = arrow_arr
                    .as_ref()
                    .as_any()
                    .downcast_ref::<daft_arrow::array::StructArray>()
                    .unwrap();
                let arrow_child_arrays = arrow_arr.values();

                let child_series = fields
                    .iter()
                    .zip(arrow_child_arrays.iter())
                    .map(|(daft_field, arrow_arr)| {
                        Series::from_arrow2(Arc::new(daft_field.clone()), arrow_arr.to_boxed())
                    })
                    .collect::<DaftResult<Vec<Series>>>()?;

                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned().map(Into::into),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft StructArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        todo!()
    }
}

impl FromArrow for MapArray {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        // we need to handle map type separately because the physical type of map in arrow2 is map but in Daft is list

        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::Map { key, value }, daft_arrow::datatypes::DataType::Map(map_field, _)) => {
                let arrow_arr = arrow_arr
                    .as_any()
                    .downcast_ref::<daft_arrow::array::MapArray>()
                    .unwrap();
                let arrow_child_array = arrow_arr.field();

                let child_field = Field::new(
                    map_field.name.clone(),
                    DataType::Struct(vec![
                        Field::new("key", *key.clone()),
                        Field::new("value", *value.clone()),
                    ]),
                );
                let physical_field = Field::new(
                    field.name.clone(),
                    DataType::List(Box::new(child_field.dtype.clone())),
                );

                let child_series =
                    Series::from_arrow2(child_field.into(), arrow_child_array.clone())?;

                let physical = ListArray::new(
                    physical_field,
                    child_series,
                    arrow_arr.offsets().into(),
                    arrow_arr.validity().cloned().map(Into::into),
                );

                Ok(Self::new(field, physical))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft MapArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        todo!()
    }
}

#[cfg(feature = "python")]
impl FromArrow for PythonArray {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        assert_eq!(field.dtype, DataType::Python);

        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow()?;

        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

        let physical_arrow_array = physical_arrow_array
            .as_any()
            .downcast_ref::<daft_arrow::array::BinaryArray<i64>>() // list array with i64 offsets
            .expect("PythonArray::from_arrow: Failed to downcast to BinaryArray<i64>");

        Self::from_iter_pickled(&field.name, physical_arrow_array.iter())
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        todo!()
    }
}

macro_rules! impl_logical_from_arrow {
    ($logical_type:ident) => {
        impl FromArrow for LogicalArray<$logical_type> {
            fn from_arrow2(
                field: FieldRef,
                arrow_arr: Box<dyn daft_arrow::array::Array>,
            ) -> DaftResult<Self> {
                let target_convert = field.to_physical();
                let target_convert_arrow = target_convert.dtype.to_arrow()?;

                let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

                let physical =
                    <<$logical_type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                        Arc::new(target_convert),
                        physical_arrow_array,
                    )?;
                Ok(Self::new(field, physical))
            }

            fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
                todo!()
            }
        }
    };
}

impl_logical_from_arrow!(DateType);
impl_logical_from_arrow!(TimeType);
impl_logical_from_arrow!(DurationType);
impl_logical_from_arrow!(ImageType);
impl_logical_from_arrow!(TimestampType);
impl_logical_from_arrow!(TensorType);
impl_logical_from_arrow!(EmbeddingType);
impl_logical_from_arrow!(FixedShapeTensorType);
impl_logical_from_arrow!(SparseTensorType);
impl_logical_from_arrow!(FixedShapeSparseTensorType);
impl_logical_from_arrow!(FixedShapeImageType);
impl<T> FromArrow for LogicalArray<FileType<T>>
where
    T: DaftMediaType,
{
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow()?;
        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);
        let physical =
            <<FileType<T> as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_schema::field::Field;
    use rstest::rstest;

    use crate::{
        array::ListArray,
        prelude::{
            Float32Array, Float64Array, FromArrow, Int8Array, Int16Array, Int32Array, Int64Array,
            UInt8Array, UInt16Array, UInt32Array, UInt64Array, *,
        },
        series,
        series::Series,
    };

    macro_rules! test_arrow_roundtrip {
        ($test_name:ident, $array_type:ty, $value:expr) => {
            #[test]
            fn $test_name() -> DaftResult<()> {
                let arr = <$array_type>::from_values("test", $value.into_iter());
                let arrow_arr = arr.to_arrow();
                let new_arr = <$array_type>::from_arrow(
                    Field::new("test", arr.data_type().clone()),
                    arrow_arr,
                )?;
                assert_eq!(arr, new_arr);

                Ok(())
            }
        };
    }

    test_arrow_roundtrip!(test_arrow_roundtrip_i8, Int8Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_u8, UInt8Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_i16, Int16Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_u16, UInt16Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_i32, Int32Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_u32, UInt32Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_i64, Int64Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_u64, UInt64Array, vec![1, 2, 3]);
    test_arrow_roundtrip!(test_arrow_roundtrip_f32, Float32Array, vec![1.0, 2.0, 3.0]);
    test_arrow_roundtrip!(test_arrow_roundtrip_f64, Float64Array, vec![1.0, 2.0, 3.0]);

    #[rstest]
    #[case(series![1u8, 2u8])]
    #[case(series![1i8, 2i8, 3i8])]
    #[case(series![1i16, 2i16, 3i16])]
    #[case(series![1i32, 2i32, 3i32])]
    #[case(series![1i64, 2i64, 3i64])]
    #[case(series![1f32, 2f32, 3f32])]
    #[case(series![1f64, 2f64, 3f64])]
    #[case(series!["a", "b", "c"])]
    #[case(series![true, false, false])]
    #[case(Series::empty("test", &DataType::Null))]
    #[case(Series::empty("test", &DataType::Utf8))]
    fn test_arrow_roundtrip_list(#[case] data: Series) -> DaftResult<()> {
        let arr = ListArray::from_series("test", vec![Some(data.clone()), None, Some(data)])?;

        let arrow_arr = arr.to_arrow()?;
        let new_arr =
            ListArray::from_arrow(Field::new("test", arr.data_type().clone()), arrow_arr)?;

        assert_eq!(arr, new_arr);

        Ok(())
    }
    #[rstest]
    #[case(series![1u8, 2u8, 3u8])]
    #[case(series![1i8, 2i8, 3i8])]
    #[case(series![1i16, 2i16, 3i16])]
    #[case(series![1i32, 2i32, 3i32])]
    #[case(series![1i64, 2i64, 3i64])]
    #[case(series![1f32, 2f32, 3f32])]
    #[case(series![1f64, 2f64, 3f64])]
    #[case(series!["a", "b", "c"])]
    #[case(series![true, false, false])]
    #[case(Series::empty("test", &DataType::Null))]
    #[case(Series::empty("test", &DataType::Utf8))]
    fn test_arrow_roundtrip_fixed_size_list(#[case] data: Series) -> DaftResult<()> {
        let arr = FixedSizeListArray::new(
            Field::new(
                "test",
                DataType::FixedSizeList(Box::new(data.data_type().clone()), 1),
            ),
            data,
            None,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr =
            FixedSizeListArray::from_arrow(Field::new("test", arr.data_type().clone()), arrow_arr)?;

        assert_eq!(arr, new_arr);

        Ok(())
    }
}
