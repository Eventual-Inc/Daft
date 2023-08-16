use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{logical::LogicalArray, DaftLogicalType, DaftPhysicalType, Field},
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

impl<L: DaftLogicalType> FromArrow for LogicalArray<L> {
    fn from_arrow(field: &Field, arrow_arr: Box<dyn arrow2::array::Array>) -> DaftResult<Self> {
        let data_array_field = Field::new(field.name.clone(), field.dtype.to_physical());
        let physical = DataArray::try_from((data_array_field, arrow_arr))?;
        Ok(LogicalArray::<L>::new(field.clone(), physical))
    }
}
