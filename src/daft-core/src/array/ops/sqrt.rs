use daft_common::error::DaftResult;
use num_traits::Float;

use crate::{array::DataArray, datatypes::DaftNumericType};

impl<T> DataArray<T>
where
    T: DaftNumericType,
    T::Native: Float,
{
    pub fn sqrt(&self) -> DaftResult<Self> {
        self.apply(|v| v.sqrt())
    }
}
