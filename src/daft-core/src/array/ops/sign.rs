use num_traits::Signed;

use crate::{array::DataArray, datatypes::DaftNumericType};

use common_error::DaftResult;

impl<T: DaftNumericType> DataArray<T>
where
    T::Native: Signed,
{
    pub fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}
