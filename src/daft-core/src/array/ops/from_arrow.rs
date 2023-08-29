use common_error::{DaftError, DaftResult};

use crate::{
    array::{DataArray, FixedSizeListArray, StructArray},
    datatypes::{logical::LogicalArray, DaftDataType, DaftLogicalType, DaftPhysicalType, Field},
    series::IntoSeries,
    with_match_daft_types, DataType, Series,
};

/// Arrays that implement [`FromArrow`] can be instantiated from a Box<dyn arrow2::array::Array>
pub trait FromArrow
where
    Self: Sized,
{
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self>;
}

impl<T: DaftPhysicalType> FromArrow for DataArray<T> {
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        DataArray::<T>::try_from((field.clone(), arrow_arr))
    }
}

impl<L: DaftLogicalType> FromArrow for LogicalArray<L>
where
    <L::PhysicalType as DaftDataType>::ArrayType: FromArrow,
{
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        let data_array_field = Field::new(field.name.clone(), field.dtype.to_physical());
        let physical_arrow_arr = arrow_arr.to_type(data_array_field.dtype.to_arrow()?);
        let physical = <L::PhysicalType as DaftDataType>::ArrayType::from_arrow(
            &data_array_field,
            physical_arrow_arr,
        )?;
        Ok(LogicalArray::<L>::new(field.clone(), physical))
    }
}

impl FromArrow for FixedSizeListArray {
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::FixedSizeList(daft_child_field, daft_size), arrow2::datatypes::DataType::FixedSizeList(_arrow_child_field, arrow_size)) => {
                if daft_size != arrow_size {
                    return Err(DaftError::TypeError(format!("Attempting to create Daft FixedSizeListArray with element length {} from Arrow FixedSizeList array with element length {}", daft_size, arrow_size)));
                }

                let arrow_arr = arrow_arr.as_ref().as_any().downcast_ref::<arrow2::array::FixedSizeListArray>().unwrap();
                let arrow_child_array = arrow_arr.values();
                let child_series = with_match_daft_types!(daft_child_field.dtype, |$T| {
                    <$T as DaftDataType>::ArrayType::from_arrow(daft_child_field.as_ref(), arrow_child_array.clone())?.into_series()
                });
                Ok(FixedSizeListArray::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned(),
                ))
            }
            (d, a) => Err(DaftError::TypeError(format!("Attempting to create Daft FixedSizeListArray with type {} from arrow array with type {:?}", d, a)))
        }
    }
}

impl FromArrow for StructArray {
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        match (&field.dtype, arrow_arr.data_type()) {
            (DataType::Struct(fields), arrow2::datatypes::DataType::Struct(arrow_fields)) => {
                if fields.len() != arrow_fields.len() {
                    return Err(DaftError::ValueError(format!("Attempting to create Daft StructArray with {} fields from Arrow array with {} fields: {} vs {:?}", fields.len(), arrow_fields.len(), &field.dtype, arrow_arr.data_type())))
                }

                let arrow_arr = arrow_arr.as_ref().as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
                let arrow_child_arrays = arrow_arr.values();

                let child_series = fields.iter().zip(arrow_child_arrays.iter()).map(|(daft_field, arrow_arr)| {
                    with_match_daft_types!(&daft_field.dtype, |$T| {
                        Ok(<$T as DaftDataType>::ArrayType::from_arrow(daft_field, arrow_arr.to_boxed())?.into_series())
                    })
                }).collect::<DaftResult<Vec<Series>>>()?;

                Ok(StructArray::new(
                    field.clone(),
                    child_series,
                    arrow_arr.validity().cloned(),
                ))
            },
            (d, a) => Err(DaftError::TypeError(format!("Attempting to create Daft StructArray with type {} from arrow array with type {:?}", d, a)))
        }
    }
}
