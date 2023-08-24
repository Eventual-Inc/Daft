mod abs;
mod apply;
mod arange;
mod arithmetic;
pub mod arrow2;
pub mod as_arrow;
pub(crate) mod broadcast;
mod cast;
mod compare_agg;
mod comparison;
mod concat;
mod concat_agg;
mod count;
mod date;
mod filter;
mod float;
pub mod from_arrow;
pub mod full;
mod get;
pub(crate) mod groups;
mod hash;
mod if_else;
pub(crate) mod image;
mod len;
mod list;
mod list_agg;
mod mean;
mod null;
mod pairwise;
mod repr;
mod search_sorted;
mod sort;
mod sum;
mod take;
pub(crate) mod tensor;
mod utf8;

pub use sort::{build_multi_array_bicompare, build_multi_array_compare};

use common_error::DaftResult;

use crate::count_mode::CountMode;

pub trait DaftCompare<Rhs> {
    type Output;

    /// equality.
    fn equal(&self, rhs: Rhs) -> Self::Output;

    /// inequality.
    fn not_equal(&self, rhs: Rhs) -> Self::Output;

    /// Greater than
    fn gt(&self, rhs: Rhs) -> Self::Output;

    /// Greater than or equal
    fn gte(&self, rhs: Rhs) -> Self::Output;

    /// Less than
    fn lt(&self, rhs: Rhs) -> Self::Output;

    /// Less than or equal
    fn lte(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftLogical<Rhs> {
    type Output;

    /// and.
    fn and(&self, rhs: Rhs) -> Self::Output;

    /// or.
    fn or(&self, rhs: Rhs) -> Self::Output;

    /// xor.
    fn xor(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftIsNull {
    type Output;
    fn is_null(&self) -> Self::Output;
}

pub trait DaftIsNan {
    type Output;
    fn is_nan(&self) -> Self::Output;
}

pub type VecIndices = Vec<u64>;
pub type GroupIndices = Vec<VecIndices>;
pub type GroupIndicesPair = (VecIndices, GroupIndices);

pub trait IntoGroups {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair>;
}

pub trait DaftCountAggable {
    type Output;
    fn count(&self, mode: CountMode) -> Self::Output;
    fn grouped_count(&self, groups: &GroupIndices, mode: CountMode) -> Self::Output;
}

pub trait DaftSumAggable {
    type Output;
    fn sum(&self) -> Self::Output;
    fn grouped_sum(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftMeanAggable {
    type Output;
    fn mean(&self) -> Self::Output;
    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftCompareAggable {
    type Output;
    fn min(&self) -> Self::Output;
    fn max(&self) -> Self::Output;
    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output;
    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftListAggable {
    type Output;
    fn list(&self) -> Self::Output;
    fn grouped_list(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftConcatAggable {
    type Output;
    fn concat(&self) -> Self::Output;
    fn grouped_concat(&self, groups: &GroupIndices) -> Self::Output;
}
