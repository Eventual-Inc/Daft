use num_traits::Float;

use crate::{array::DataArray, datatypes::DaftFloatType};

use common_error::DaftResult;

impl<T> DataArray<T>
where
    T: DaftFloatType,
    T::Native: Float,
{
    pub fn log2(&self) -> DaftResult<Self> {
        self.apply(|v| v.log2())
    }

    pub fn log10(&self) -> DaftResult<Self> {
        self.apply(|v| v.log10())
    }

    pub fn log(&self, base: T::Native) -> DaftResult<Self> {
        self.apply(|v| v.log(base))
    }

    pub fn ln(&self) -> DaftResult<Self> {
        self.apply(|v| v.ln())
    }
}
