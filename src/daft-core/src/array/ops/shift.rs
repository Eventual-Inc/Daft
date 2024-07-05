use std::ops::{Shl, Shr};

use common_error::DaftResult;
use num_traits::PrimInt;

use crate::{
    array::DataArray,
    datatypes::{DaftIntegerType, UInt64Type},
};

impl<T> DataArray<T>
where
    T: DaftIntegerType,
    T::Native: PrimInt,
{
    pub fn shift_left(&self, rhs: &DataArray<UInt64Type>) -> DaftResult<Self> {
        self.binary_apply(rhs, |lhs, rhs| lhs.shl(rhs as usize))
    }

    pub fn shift_right(&self, rhs: &DataArray<UInt64Type>) -> DaftResult<Self> {
        self.binary_apply(rhs, |lhs, rhs| lhs.shr(rhs as usize))
    }
}
