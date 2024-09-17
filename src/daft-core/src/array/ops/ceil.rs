use common_error::DaftResult;
use num_traits::Float;

use crate::{
    array::DataArray,
    datatypes::{DaftFloatType, DaftNumericType},
};

impl<T: DaftFloatType> DataArray<T>
where
    T: DaftNumericType,
    T::Native: Float,
{
    pub fn ceil(&self) -> DaftResult<Self> {
        self.apply(|v| v.ceil())
    }
}
