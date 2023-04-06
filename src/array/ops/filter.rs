use crate::{
    array::DataArray,
    datatypes::{BinaryArray, BooleanArray, DaftNumericType, NullArray, Utf8Array},
    error::DaftResult,
};

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl Utf8Array {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl BinaryArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl BooleanArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl NullArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let set_bits = mask.len() - mask.downcast().values().unset_bits();
        Ok(NullArray::full_null(self.name(), set_bits))
    }
}
