use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftNumericType, NullArray, UInt64Array, Utf8Array},
    error::DaftResult,
    kernels,
};

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let downcasted = self.downcast();

        let seed = seed.map(|v| v.downcast());

        let result = kernels::hashing::hash(downcasted, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl Utf8Array {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let downcasted = self.downcast();

        let seed = seed.map(|v| v.downcast());

        let result = kernels::hashing::hash(downcasted, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl BooleanArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let downcasted = self.downcast();

        let seed = seed.map(|v| v.downcast());

        let result = kernels::hashing::hash(downcasted, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl NullArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let downcasted = self.data();

        let seed = seed.map(|v| v.downcast());

        let result = kernels::hashing::hash(downcasted, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}
