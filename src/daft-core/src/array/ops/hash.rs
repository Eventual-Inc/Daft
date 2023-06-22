use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftNumericType, NullArray, UInt64Array, Utf8Array},
    kernels,
};

use common_error::DaftResult;

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl Utf8Array {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl BooleanArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.as_arrow();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}

impl NullArray {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let as_arrowed = self.data();

        let seed = seed.map(|v| v.as_arrow());

        let result = kernels::hashing::hash(as_arrowed, seed)?;

        Ok(DataArray::from((self.name(), Box::new(result))))
    }
}
