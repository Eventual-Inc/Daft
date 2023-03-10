use crate::{array::DataArray, datatypes::DaftNumericType, error::DaftResult};

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn slice(&self, offset: usize, length: usize) -> DaftResult<Self> {
        Ok(DataArray::from((self.name(), Box::new(self.downcast().slice(offset, length)))))
    }
}