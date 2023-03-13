use num_traits::Signed;

use crate::{array::DataArray, datatypes::DaftNumericType, error::DaftResult};

impl<T: DaftNumericType> DataArray<T>
where
    T::Native: Signed,
{
    pub fn abs(&self) -> DaftResult<Self> {
        self.apply(|v| v.abs())
    }
}
