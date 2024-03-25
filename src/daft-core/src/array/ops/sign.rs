use num_traits::Signed;

use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, UInt16Array, UInt32Array, UInt64Array, UInt8Array},
};

use common_error::DaftResult;

impl<T: DaftNumericType> DataArray<T>
where
    T::Native: Signed,
{
    pub fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl UInt8Array {
    pub fn sign_unsigned(&self) -> DaftResult<Self> {
        self.apply(|v| if v == 0 { 0 } else { 1 })
    }
}

impl UInt16Array {
    pub fn sign_unsigned(&self) -> DaftResult<Self> {
        self.apply(|v| if v == 0 { 0 } else { 1 })
    }
}

impl UInt32Array {
    pub fn sign_unsigned(&self) -> DaftResult<Self> {
        self.apply(|v| if v == 0 { 0 } else { 1 })
    }
}

impl UInt64Array {
    pub fn sign_unsigned(&self) -> DaftResult<Self> {
        self.apply(|v| if v == 0 { 0 } else { 1 })
    }
}
