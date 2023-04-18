mod abs;
mod apply;
mod arange;
mod arithmetic;
pub mod arrow2;
pub(crate) mod broadcast;
mod cast;
mod compare_agg;
mod comparison;
mod concat;
mod count;
mod date;
pub(crate) mod downcast;
mod filter;
mod float;
mod full;
mod hash;
mod if_else;
mod len;
mod mean;
mod null;
mod pairwise;
mod search_sorted;
mod sort;
mod sum;
mod take;
mod utf8;

pub use sort::{build_multi_array_bicompare, build_multi_array_compare};

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

pub type GroupIndices = Vec<Vec<u64>>;

pub trait DaftCountAggable {
    type Output;
    fn count(&self) -> Self::Output;
    fn grouped_count(&self, groups: &GroupIndices) -> Self::Output;
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
