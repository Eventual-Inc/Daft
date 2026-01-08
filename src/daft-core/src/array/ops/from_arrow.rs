use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
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
        Self::new_from_arrow2(field.into(), arrow_arr.into())
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
        let child_series = Series::from_arrow(
            Arc::new(Field::new("list", daft_child_dtype.as_ref().clone())),
            arrow_child_array.clone(),
        )?;

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
        let field: FieldRef = field.into();

        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::Struct(fields), arrow::datatypes::DataType::Struct(arrow_fields)) => {
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
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .unwrap();

                let child_series = fields
                    .iter()
                    .zip(arrow_arr.columns().iter())
                    .map(|(daft_field, arrow_arr)| {
                        Series::from_arrow(Arc::new(daft_field.clone()), arrow_arr.clone())
                    })
                    .collect::<DaftResult<Vec<Series>>>()?;

                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.nulls().cloned().map(Into::into),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!(
                "Attempting to create Daft StructArray with type {} from arrow array with type {:?}",
                d, a
            ))),
        }
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
        let field: FieldRef = field.into();

        let DataType::Map { key, value } = &field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected Map DataType, got {}",
                field.dtype
            )));
        };

        let arrow::datatypes::DataType::Map(map_field, _) = arrow_arr.data_type() else {
            return Err(DaftError::TypeError(format!(
                "Attempting to create Daft MapArray from arrow array with type {:?}",
                arrow_arr.data_type()
            )));
        };

        let arrow_arr = arrow_arr
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();

        let arrow_child_array: ArrayRef = Arc::new(arrow_arr.entries().clone());

        let child_field = Field::new(
            map_field.name(),
            DataType::Struct(vec![
                Field::new("key", *key.clone()),
                Field::new("value", *value.clone()),
            ]),
        );
        let physical_field = Field::new(
            field.name.clone(),
            DataType::List(Box::new(child_field.dtype.clone())),
        );

        let child_series = Series::from_arrow(child_field.into(), arrow_child_array.clone())?;

        let offsets: arrow::buffer::Buffer = arrow_arr.offsets().inner().clone().into_inner();
        let offsets =
            unsafe { daft_arrow::offset::OffsetsBuffer::<i64>::new_unchecked(offsets.into()) };
        let validity = arrow_arr.nulls().cloned();

        let physical = ListArray::new(physical_field, child_series, offsets, validity);

        Ok(Self::new(field, physical))
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
        let target_convert_arrow = target_convert.dtype.to_arrow2()?;

        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

        let physical_arrow_array = physical_arrow_array
            .as_any()
            .downcast_ref::<daft_arrow::array::BinaryArray<i64>>() // list array with i64 offsets
            .expect("PythonArray::from_arrow: Failed to downcast to BinaryArray<i64>");

        Self::from_iter_pickled(&field.name, physical_arrow_array.iter())
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field: FieldRef = field.into();
        assert_eq!(field.dtype, DataType::Python);

        let physical_arrow_array =
            arrow::compute::cast(arrow_arr.as_ref(), &DataType::Binary.to_arrow()?)?;

        let physical_arrow_array = physical_arrow_array
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .expect("PythonArray::from_arrow: Failed to downcast to LargeBinaryArray");
        let v = physical_arrow_array
            .iter()
            .map(|x| x.map(|x| x.to_vec()))
            .collect::<Vec<_>>();
        // trustedlen forces materialization here
        // TODO: remove once we dont have arrow2 anymore
        Self::from_iter_pickled(&field.name, v.into_iter())
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
                let target_convert_arrow = target_convert.dtype.to_arrow2()?;

                let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

                let physical =
                    <<$logical_type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                        Arc::new(target_convert),
                        physical_arrow_array,
                    )?;
                Ok(Self::new(field, physical))
            }

            fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
                let field: FieldRef = field.into();
                  let target_convert = field.to_physical();
                  let target_convert_arrow = target_convert.dtype.to_arrow()?;

                  let physical_arrow_array = arrow::compute::cast(
                      arrow_arr.as_ref(),
                      &target_convert_arrow,
                  )?;

                  let physical =
                      <<$logical_type as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow(
                          Arc::new(target_convert),
                          physical_arrow_array,
                      )?;
                  Ok(Self::new(field, physical))
            }
        }
    };
}

impl FromArrow for LogicalArray<DateType> {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow2()?;
        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);
        let physical =
            <<DateType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }
    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field = field.into();
        let target_convert = field.to_physical();
        let physical_arrow_array = Arc::new(
            arrow_arr
                .as_primitive::<arrow::datatypes::Date32Type>()
                .reinterpret_cast::<arrow::datatypes::Int32Type>(),
        );

        let physical =
            <<DateType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }
}

impl FromArrow for LogicalArray<TimeType> {
    fn from_arrow2(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow2()?;
        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);
        let physical =
            <<TimeType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }
    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field = field.into();
        let target_convert = field.to_physical();
        let physical_arrow_array = Arc::new(
            arrow_arr
                .as_primitive::<arrow::datatypes::Time64NanosecondType>()
                .reinterpret_cast::<arrow::datatypes::Int64Type>(),
        );

        let physical =
            <<TimeType as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }
}
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
        let target_convert_arrow = target_convert.dtype.to_arrow2()?;
        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);
        let physical =
            <<FileType<T> as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow2(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }

    fn from_arrow<F: Into<FieldRef>>(field: F, arrow_arr: ArrayRef) -> DaftResult<Self> {
        let field: FieldRef = field.into();
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow()?;

        let physical_arrow_array = arrow::compute::cast(arrow_arr.as_ref(), &target_convert_arrow)?;

        let physical =
               <<FileType<T> as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow(
                   Arc::new(target_convert),
                   physical_arrow_array,
               )?;
        Ok(Self::new(field, physical))
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
        let new_arr = ListArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.validity(), new_arr.validity());
        assert_eq!(arr.flat_child, new_arr.flat_child);

        Ok(())
    }

    #[rstest]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Utf8))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Null))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Image(None)))))]
    fn test_arrow_roundtrip_nested_list(#[case] data: Series) -> DaftResult<()> {
        let arr = ListArray::from_series("test", vec![Some(data.clone()), None, Some(data)])?;

        let arrow_arr = arr.to_arrow()?;
        let new_arr = ListArray::from_arrow(arr.field().clone(), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.offsets(), new_arr.offsets());
        assert_eq!(arr.validity(), new_arr.validity());

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
        let field = Field::new("test", arr.data_type().clone());
        let new_arr = FixedSizeListArray::from_arrow(field, arrow_arr)?;

        assert_eq!(arr, new_arr);

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_date() -> DaftResult<()> {
        let arr = LogicalArray::<DateType>::new(
            Field::new("test", DataType::Date),
            Int32Array::from_values("", vec![1, 2, 3].into_iter()),
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = LogicalArray::<DateType>::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.physical, new_arr.physical);

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
    fn test_arrow_roundtrip_logical_embedding(#[case] data: Series) -> DaftResult<()> {
        let arr = FixedSizeListArray::new(
            Field::new(
                "",
                DataType::FixedSizeList(Box::new(data.data_type().clone()), 1),
            ),
            data,
            None,
        );
        let embedding_array = EmbeddingArray::new(
            Field::new(
                "",
                DataType::Embedding(Box::new(arr.child_data_type().clone()), 1),
            ),
            arr,
        );

        let arrow_arr = embedding_array.to_arrow()?;
        let new_arr = EmbeddingArray::from_arrow(
            Field::new("", embedding_array.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(embedding_array.physical, new_arr.physical);
        assert_eq!(embedding_array.field(), new_arr.field());

        Ok(())
    }
    #[rstest]
    #[case(Series::empty("test", &DataType::Null))]
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
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Null))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Utf8))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Int32))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Float32))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Float64))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Boolean))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Timestamp(TimeUnit::Nanoseconds, None)))))]
    #[case(Series::empty("test", &DataType::List(Box::new(DataType::Date))))]
    fn test_arrow_roundtrip_struct(#[case] data: Series) -> DaftResult<()> {
        let arr = StructArray::new(
            Field::new("item1", DataType::Struct(vec![data.field().clone()])),
            vec![data],
            None,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = StructArray::from_arrow(arr.field().clone(), arrow_arr)?;
        assert_eq!(arr.len(), new_arr.len());
        assert_eq!(arr.validity(), new_arr.validity());
        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_duration() -> DaftResult<()> {
        let arr = LogicalArray::<DurationType>::new(
            Field::new("test", DataType::Duration(TimeUnit::Milliseconds)),
            Int64Array::from_values("", vec![1000, 2000, 3000].into_iter()),
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = LogicalArray::<DurationType>::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.physical, new_arr.physical);

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_timestamp() -> DaftResult<()> {
        let arr = LogicalArray::<TimestampType>::new(
            Field::new(
                "test",
                DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string())),
            ),
            Int64Array::from_values("", vec![1000000, 2000000, 3000000].into_iter()),
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = LogicalArray::<TimestampType>::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(arr.field(), new_arr.field());
        assert_eq!(arr.physical, new_arr.physical);

        Ok(())
    }
    #[test]
    fn test_arrow_roundtrip_logical_image() -> DaftResult<()> {
        let struct_array = StructArray::new(
            Field::new(
                "",
                DataType::Struct(vec![
                    Field::new("data", DataType::List(Box::new(DataType::UInt8))),
                    Field::new("channel", DataType::UInt16),
                    Field::new("height", DataType::UInt32),
                    Field::new("width", DataType::UInt32),
                    Field::new("mode", DataType::UInt8),
                ]),
            ),
            vec![
                Series::empty("data", &DataType::List(Box::new(DataType::UInt8))),
                Series::empty("channel", &DataType::UInt16),
                Series::empty("height", &DataType::UInt32),
                Series::empty("width", &DataType::UInt32),
                Series::empty("mode", &DataType::UInt8),
            ],
            None,
        );

        let arr = ImageArray::new(Field::new("test", DataType::Image(None)), struct_array);

        let arrow_arr = arr.to_arrow()?;
        let new_arr =
            ImageArray::from_arrow(Field::new("test", arr.data_type().clone()), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_fixed_shape_image() -> DaftResult<()> {
        let height = 2;
        let width = 2;
        let channels = 3; // RGB
        let size = (height * width * channels) as usize;

        let data = Series::from_literals(vec![Literal::UInt8(0u8); size])?;
        let list_array = FixedSizeListArray::new(
            Field::new("", DataType::FixedSizeList(Box::new(DataType::UInt8), size)),
            data,
            None,
        );

        let arr = FixedShapeImageArray::new(
            Field::new(
                "test",
                DataType::FixedShapeImage(ImageMode::RGB, height, width),
            ),
            list_array,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = FixedShapeImageArray::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;
        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_tensor() -> DaftResult<()> {
        let struct_array = StructArray::new(
            Field::new(
                "",
                DataType::Struct(vec![
                    Field::new("data", DataType::List(Box::new(DataType::Float32))),
                    Field::new("shape", DataType::List(Box::new(DataType::UInt64))),
                ]),
            ),
            vec![
                Series::empty("data", &DataType::List(Box::new(DataType::Float32))),
                Series::empty("shape", &DataType::List(Box::new(DataType::UInt64))),
            ],
            None,
        );

        let arr = TensorArray::new(
            Field::new("test", DataType::Tensor(Box::new(DataType::Float32))),
            struct_array,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr =
            TensorArray::from_arrow(Field::new("test", arr.data_type().clone()), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_fixed_shape_tensor() -> DaftResult<()> {
        let shape = vec![3u64, 224u64, 224u64];
        let size: usize = shape.clone().iter().map(|v| *v as usize).product();

        let data = Series::from_literals(vec![Literal::Float32(0.0f32); size])?;

        let list_array = FixedSizeListArray::new(
            Field::new(
                "",
                DataType::FixedSizeList(Box::new(DataType::Float32), size),
            ),
            data,
            None,
        );

        let arr = FixedShapeTensorArray::new(
            Field::new(
                "test",
                DataType::FixedShapeTensor(Box::new(DataType::Float32), shape),
            ),
            list_array,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = FixedShapeTensorArray::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_sparse_tensor() -> DaftResult<()> {
        let struct_array = StructArray::new(
            Field::new(
                "",
                DataType::Struct(vec![
                    Field::new("values", DataType::List(Box::new(DataType::Float64))),
                    Field::new("indices", DataType::List(Box::new(DataType::UInt64))),
                    Field::new("shape", DataType::List(Box::new(DataType::UInt64))),
                ]),
            ),
            vec![
                Series::empty("values", &DataType::List(Box::new(DataType::Float64))),
                Series::empty("indices", &DataType::List(Box::new(DataType::UInt64))),
                Series::empty("shape", &DataType::List(Box::new(DataType::UInt64))),
            ],
            None,
        );

        let arr = SparseTensorArray::new(
            Field::new(
                "test",
                DataType::SparseTensor(Box::new(DataType::Float64), true),
            ),
            struct_array,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr =
            SparseTensorArray::from_arrow(Field::new("test", arr.data_type().clone()), arrow_arr)?;

        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_arrow_roundtrip_logical_fixed_shape_sparse_tensor() -> DaftResult<()> {
        let struct_array = StructArray::new(
            Field::new(
                "",
                DataType::Struct(vec![
                    Field::new("values", DataType::List(Box::new(DataType::Float32))),
                    Field::new("indices", DataType::List(Box::new(DataType::UInt16))),
                ]),
            ),
            vec![
                Series::empty("values", &DataType::List(Box::new(DataType::Float32))),
                Series::empty("indices", &DataType::List(Box::new(DataType::UInt16))),
            ],
            None,
        );

        let arr = FixedShapeSparseTensorArray::new(
            Field::new(
                "test",
                DataType::FixedShapeSparseTensor(Box::new(DataType::Float32), vec![100, 100], true),
            ),
            struct_array,
        );

        let arrow_arr = arr.to_arrow()?;
        let new_arr = FixedShapeSparseTensorArray::from_arrow(
            Field::new("test", arr.data_type().clone()),
            arrow_arr,
        )?;

        assert_eq!(arr.field(), new_arr.field());
        for i in 0..arr.len() {
            let expected = arr.get_lit(i);
            let actual = new_arr.get_lit(i);
            assert_eq!(expected, actual);
        }

        Ok(())
    }
}
