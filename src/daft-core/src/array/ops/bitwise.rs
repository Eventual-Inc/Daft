use std::ops::{BitAnd, BitOr, BitXor};

use common_error::DaftResult;

use super::DaftLogical;
use crate::{
    array::DataArray,
    datatypes::{DaftIntegerType, DaftNumericType},
};

impl<T> DaftLogical<&Self> for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native:
        Ord + BitAnd<Output = T::Native> + BitOr<Output = T::Native> + BitXor<Output = T::Native>,
{
    type Output = DaftResult<Self>;

    fn and(&self, rhs: &Self) -> Self::Output {
        self.binary_apply(rhs, |lhs, rhs| lhs.bitand(rhs))
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        self.binary_apply(rhs, |lhs, rhs| lhs.bitor(rhs))
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        self.binary_apply(rhs, |lhs, rhs| lhs.bitxor(rhs))
    }
}
