use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_arrow::{array::Array, compute::cast::cast};

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
    fn from_arrow(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self>;
}

impl<T: DaftPhysicalType> FromArrow for DataArray<T> {
    fn from_arrow(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        Self::try_from((field, arrow_arr))
    }
}

impl FromArrow for FixedSizeListArray {
    fn from_arrow(
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
                let child_series = Series::from_arrow(
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
}

impl FromArrow for ListArray {
    fn from_arrow(
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
                let child_series = Series::from_arrow(
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
}

impl FromArrow for StructArray {
    fn from_arrow(
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
                        Series::from_arrow(Arc::new(daft_field.clone()), arrow_arr.to_boxed())
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
}

impl FromArrow for MapArray {
    fn from_arrow(
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
                    Series::from_arrow(child_field.into(), arrow_child_array.clone())?;

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
}

#[cfg(feature = "python")]
impl FromArrow for PythonArray {
    fn from_arrow(
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
}

macro_rules! impl_logical_from_arrow {
    ($logical_type:ident) => {
        impl FromArrow for LogicalArray<$logical_type> {
            fn from_arrow(
                field: FieldRef,
                arrow_arr: Box<dyn daft_arrow::array::Array>,
            ) -> DaftResult<Self> {
                let target_convert = field.to_physical();
                let target_convert_arrow = target_convert.dtype.to_arrow2()?;

                let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

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
    fn from_arrow(
        field: FieldRef,
        arrow_arr: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow2()?;
        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);
        let physical =
            <<FileType<T> as DaftLogicalType>::PhysicalType as DaftDataType>::ArrayType::from_arrow(
                Arc::new(target_convert),
                physical_arrow_array,
            )?;
        Ok(Self::new(field, physical))
    }
}
