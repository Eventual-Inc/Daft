use num_traits::Float;

use crate::{
    array::DataArray,
    datatypes::{DaftFloatType, DaftNumericType},
};

use common_error::DaftResult;

impl<T: DaftFloatType> DataArray<T>
where
    T: DaftNumericType,
    T::Native: Float,
{
    pub fn log2(&self) -> DaftResult<Self> {
        self.apply(|v| v.log2())
    }

    pub fn log10(&self) -> DaftResult<Self> {
        self.apply(|v| v.log10())
    }

    pub fn ln(&self) -> DaftResult<Self> {
        self.apply(|v| v.ln())
    }
}
