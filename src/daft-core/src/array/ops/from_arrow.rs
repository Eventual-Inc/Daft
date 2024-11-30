use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        logical::LogicalArray, DaftDataType, DaftLogicalType, DaftPhysicalType, DataType, Field,
        FieldRef,
    },
    series::Series,
};

/// Arrays that implement [`FromArrow`] can be instantiated from a Box<dyn arrow2::array::Array>
pub trait FromArrow
where
    Self: Sized,
{
    fn from_arrow(field: FieldRef, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self>;
}

impl<T: DaftPhysicalType> FromArrow for DataArray<T> {
    fn from_arrow(field: FieldRef, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        Self::try_from((field, arrow_arr))
    }
}

impl<L: DaftLogicalType> FromArrow for LogicalArray<L>
where
    <L::PhysicalType as DaftDataType>::ArrayType: FromArrow,
{
    fn from_arrow(field: FieldRef, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        let target_convert = field.to_physical();
        let target_convert_arrow = target_convert.dtype.to_arrow()?;

        let physical_arrow_array = arrow_arr.convert_logical_type(target_convert_arrow);

        let physical = <L::PhysicalType as DaftDataType>::ArrayType::from_arrow(
            Arc::new(target_convert),
            physical_arrow_array,
        )?;
        Ok(Self::new(field, physical))
    }
}

impl FromArrow for FixedSizeListArray {
    fn from_arrow(field: FieldRef, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::FixedSizeList(daft_child_dtype, daft_size), arrow2::datatypes::DataType::FixedSizeList(_arrow_child_field, arrow_size)) => {
                if daft_size != arrow_size {
                    return Err(DaftError::TypeError(format!("Attempting to create Daft FixedSizeListArray with element length {} from Arrow FixedSizeList array with element length {}", daft_size, arrow_size)));
                }

                let arrow_arr = arrow_arr.as_ref().as_any().downcast_ref::<arrow2::array::FixedSizeListArray>().unwrap();
                let arrow_child_array = arrow_arr.values();
                let child_series = Series::from_arrow(Arc::new(Field::new("item", daft_child_dtype.as_ref().clone())), arrow_child_array.clone())?;
                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned(),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!("Attempting to create Daft FixedSizeListArray with type {} from arrow array with type {:?}", d, a)))
        }
    }
}

impl FromArrow for ListArray {
    fn from_arrow(
        target_field: FieldRef,
        arrow_arr: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        let target_dtype = &target_field.dtype;
        let arrow_dtype = arrow_arr.data_type();

        let result = match (target_dtype, arrow_dtype) {
            (
                DataType::List(daft_child_dtype),
                arrow2::datatypes::DataType::List(arrow_child_field),
            )
            | (
                DataType::List(daft_child_dtype),
                arrow2::datatypes::DataType::LargeList(arrow_child_field),
            ) => {
                // unifying lists
                let arrow_arr = arrow_arr.convert_logical_type(
                    arrow2::datatypes::DataType::LargeList(arrow_child_field.clone()),
                );

                let arrow_arr = arrow_arr
                    .as_any()
                    .downcast_ref::<arrow2::array::ListArray<i64>>() // list array with i64 offsets
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
                    arrow_arr.validity().cloned(),
                ))
            }
            (DataType::List(daft_child_dtype), arrow2::datatypes::DataType::Map { .. }) => {
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
    fn from_arrow(field: FieldRef, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::Struct(fields), arrow2::datatypes::DataType::Struct(arrow_fields)) => {
                if fields.len() != arrow_fields.len() {
                    return Err(DaftError::ValueError(format!("Attempting to create Daft StructArray with {} fields from Arrow array with {} fields: {} vs {:?}", fields.len(), arrow_fields.len(), &field.dtype, arrow_arr.data_type())));
                }

                let arrow_arr = arrow_arr.as_ref().as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
                let arrow_child_arrays = arrow_arr.values();

                let child_series = fields.iter().zip(arrow_child_arrays.iter()).map(|(daft_field, arrow_arr)| {
                    Series::from_arrow(Arc::new(daft_field.clone()), arrow_arr.to_boxed())
                }).collect::<DaftResult<Vec<Series>>>()?;

                Ok(Self::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned(),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!("Attempting to create Daft StructArray with type {} from arrow array with type {:?}", d, a)))
        }
    }
}
