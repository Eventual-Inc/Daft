use common_error::DaftResult;
use num_traits::Float;

use crate::{array::DataArray, datatypes::DaftFloatType};

impl<T> DataArray<T>
where
    T: DaftFloatType,
    T::Native: Float,
{
    pub fn pow(&self, exp: T::Native) -> DaftResult<Self> {
        self.apply(|v| v.powf(exp))
    }
}
