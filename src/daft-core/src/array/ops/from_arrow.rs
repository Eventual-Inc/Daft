use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{logical::LogicalArray, DaftDataType, DaftLogicalType, DaftPhysicalType, Field},
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
