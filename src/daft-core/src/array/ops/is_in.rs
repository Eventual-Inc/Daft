use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, Float32Array, Float64Array,
        NullArray, Utf8Array,
    },
};

use super::DaftIsIn;
use common_error::DaftResult;
use std::hash::Hash;

use super::as_arrow::AsArrow;

macro_rules! collect_to_hashset_and_check_membership {
    ($self:expr, $rhs:expr) => {{
        let hashset = $rhs
            .as_arrow()
            .iter()
            .collect::<std::collections::HashSet<_>>();
        let result = $self
            .as_arrow()
            .iter()
            .map(|x| hashset.contains(&x))
            .collect::<Vec<_>>();
        Ok(BooleanArray::from(($self.name(), result.as_slice())))
    }};
}

impl<T> DaftIsIn<&DataArray<T>> for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
    <T as DaftNumericType>::Native: Hash,
    <T as DaftNumericType>::Native: std::cmp::Eq,
{
    type Output = DaftResult<BooleanArray>;

    fn is_in(&self, rhs: &DataArray<T>) -> Self::Output {
        collect_to_hashset_and_check_membership!(self, rhs)
    }
}

macro_rules! impl_is_in_floating_array {
    ($arr:ident) => {
        impl DaftIsIn<&$arr> for $arr {
            type Output = DaftResult<BooleanArray>;

            fn is_in(&self, rhs: &$arr) -> Self::Output {
                // collect to vec because floats don't implement Hash
                let vec = rhs.as_arrow().iter().collect::<Vec<_>>();
                let result = self
                    .as_arrow()
                    .iter()
                    .map(|x| vec.contains(&x))
                    .collect::<Vec<_>>();
                Ok(BooleanArray::from((self.name(), result.as_slice())))
            }
        }
    };
}
impl_is_in_floating_array!(Float32Array);
impl_is_in_floating_array!(Float64Array);

macro_rules! impl_is_in_non_numeric_array {
    ($arr:ident) => {
        impl DaftIsIn<&$arr> for $arr {
            type Output = DaftResult<BooleanArray>;

            fn is_in(&self, rhs: &$arr) -> Self::Output {
                collect_to_hashset_and_check_membership!(self, rhs)
            }
        }
    };
}
impl_is_in_non_numeric_array!(BooleanArray);
impl_is_in_non_numeric_array!(Utf8Array);
impl_is_in_non_numeric_array!(BinaryArray);

impl DaftIsIn<&NullArray> for NullArray {
    type Output = DaftResult<BooleanArray>;

    fn is_in(&self, _rhs: &NullArray) -> Self::Output {
        // If self and rhs are null array then membership is always true
        Ok(BooleanArray::from((
            self.name(),
            vec![true; self.len()].as_slice(),
        )))
    }
}
