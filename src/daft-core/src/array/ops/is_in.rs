use std::collections::{BTreeSet, HashSet};

use common_error::DaftResult;
use common_hashable_float_wrapper::FloatWrapper;

use super::{DaftIsIn, as_arrow::AsArrow, full::FullNull};
use crate::{
    array::{DataArray, prelude::*},
    datatypes::prelude::*,
};

macro_rules! collect_to_set_and_check_membership {
    ($self:expr, $rhs:expr) => {{
        let rhs_arrow = $rhs.as_arrow()?;
        let set = rhs_arrow
            .iter()
            .filter_map(|item| item)
            .collect::<HashSet<_>>();

        let self_arrow = $self.as_arrow()?;
        let result = self_arrow
            .iter()
            .map(|option| option.map(|value| set.contains(&value)));

        let bools: BooleanArray = result.collect();
        Ok(bools.rename($self.name()))
    }};
}

impl<T> DaftIsIn<&Self> for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
    <<<T as DaftNumericType>::Native as crate::datatypes::NumericNative>::ARROWTYPE as arrow::array::ArrowPrimitiveType>::Native:
        std::hash::Hash + std::cmp::Eq,
{
    type Output = DaftResult<BooleanArray>;

    fn is_in(&self, rhs: &Self) -> Self::Output {
        collect_to_set_and_check_membership!(self, rhs)
    }
}

macro_rules! impl_is_in_floating_array {
    ($arr:ident, $T:ident) => {
        impl DaftIsIn<&$arr> for $arr {
            type Output = DaftResult<BooleanArray>;

            fn is_in(&self, rhs: &$arr) -> Self::Output {
                let rhs_arrow = rhs.as_arrow()?;
                let set = rhs_arrow
                    .iter()
                    .filter_map(|item| item.map(FloatWrapper))
                    .collect::<BTreeSet<FloatWrapper<$T>>>();

                let self_arrow = self.as_arrow()?;
                let result = self_arrow
                    .iter()
                    .map(|option| option.map(|value| set.contains(&FloatWrapper(value))));

                let bools: BooleanArray = result.collect();
                Ok(bools.rename(self.name()))
            }
        }
    };
}
impl_is_in_floating_array!(Float32Array, f32);
impl_is_in_floating_array!(Float64Array, f64);

macro_rules! impl_is_in_non_numeric_array {
    ($arr:ident) => {
        impl DaftIsIn<&$arr> for $arr {
            type Output = DaftResult<BooleanArray>;

            fn is_in(&self, rhs: &$arr) -> Self::Output {
                collect_to_set_and_check_membership!(self, rhs)
            }
        }
    };
}

impl_is_in_non_numeric_array!(Decimal128Array);
impl_is_in_non_numeric_array!(BooleanArray);
impl_is_in_non_numeric_array!(Utf8Array);
impl_is_in_non_numeric_array!(BinaryArray);
impl_is_in_non_numeric_array!(FixedSizeBinaryArray);

impl DaftIsIn<&Self> for NullArray {
    type Output = DaftResult<BooleanArray>;

    fn is_in(&self, _rhs: &Self) -> Self::Output {
        // If self and rhs are null array then return a full null array
        Ok(BooleanArray::full_null(
            self.name(),
            &DataType::Boolean,
            self.len(),
        ))
    }
}
