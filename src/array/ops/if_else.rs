use crate::datatypes::DaftDataType;
use crate::{array::DataArray, datatypes::BooleanArray, error::DaftResult};
use arrow2::compute::if_then_else::if_then_else;

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn if_else(self, other: DataArray<T>, predicate: BooleanArray) -> DaftResult<DataArray<T>> {
        let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
        DataArray::try_from((self.name(), result))
    }
}
