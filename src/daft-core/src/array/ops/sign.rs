use num_traits::Float;

use crate::{
    array::DataArray,
    datatypes::{DaftFloatType, DaftNumericType, Int16Array, Int32Array, Int64Array, Int8Array},
};

use common_error::DaftResult;

use super::SignOp;

impl<T: DaftFloatType> SignOp for DataArray<T>
where
    T: DaftNumericType,
    T::Native: Float,
{
    fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl SignOp for Int8Array {
    fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl SignOp for Int16Array {
    fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl SignOp for Int32Array {
    fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}

impl SignOp for Int64Array {
    fn sign(&self) -> DaftResult<Self> {
        self.apply(|v| v.signum())
    }
}
