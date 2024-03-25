use crate::{array::DataArray, datatypes::DaftNumericType};
use num_traits::Signed;
use num_traits::Unsigned;
use num_traits::{One, Zero};

use common_error::DaftResult;

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
