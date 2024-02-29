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
    pub fn floor(&self) -> DaftResult<Self> {
        self.apply(|v| v.floor())
    }
}
