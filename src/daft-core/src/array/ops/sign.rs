use common_error::DaftResult;
use num_traits::{One, Signed, Unsigned, Zero};

use crate::{array::DataArray, datatypes::DaftNumericType};

impl<T: DaftNumericType> DataArray<T>
where
    T::Native: Signed,
{
    pub fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl<T: DaftNumericType> DataArray<T>
where
    T::Native: Unsigned,
{
    pub fn sign_unsigned(&self) -> DaftResult<Self> {
        self.apply(|v| {
            if v.is_zero() {
                T::Native::zero()
            } else {
                T::Native::one()
            }
        })
    }
}
