mod abs;
mod apply;
mod approx_count_distinct;
mod approx_sketch;
mod arange;
mod arithmetic;
pub mod arrow2;
pub mod as_arrow;
mod between;
mod bitwise;
mod bool_agg;
pub(crate) mod broadcast;
pub(crate) mod cast;
mod cbrt;
mod ceil;
mod clip;
mod compare_agg;
mod comparison;
mod concat;
mod concat_agg;
mod count;
mod exp;
mod filter;
mod float;
mod floor;
pub mod from_arrow;
pub mod full;
mod get;
pub(crate) mod groups;
mod hash;
mod hll_merge;
mod hll_sketch;
mod if_else;
mod is_in;
mod len;
mod list_agg;
mod log;
mod map;
mod mean;
mod merge_sketch;
mod minhash;
mod null;
mod pairwise;
mod repr;
mod round;
mod search_sorted;
mod shift;
mod sign;
mod sketch_percentile;
mod skew;
mod sort;
pub(crate) mod sparse_tensor;
mod sqrt;
mod stddev;
mod struct_;
mod sum;
mod take;
pub(crate) mod tensor;
mod time;
pub mod trigonometry;
mod truncate;
mod utf8;

use std::hash::BuildHasher;

use common_error::DaftResult;
pub use hll_sketch::HLL_SKETCH_DTYPE;
pub use sort::{build_multi_array_bicompare, build_multi_array_compare};

use crate::count_mode::CountMode;

pub trait DaftCompare<Rhs> {
    type Output;

    /// equality.
    fn equal(&self, rhs: Rhs) -> Self::Output;

    /// null-safe equality.
    fn eq_null_safe(&self, rhs: Rhs) -> Self::Output;

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

pub trait DaftIsIn<Rhs> {
    type Output;
    fn is_in(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftBetween<Lower, Upper> {
    type Output;
    fn between(&self, lower: Lower, upper: Upper) -> Self::Output;
}

pub trait DaftAtan2<Rhs> {
    type Output;
    fn atan2(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftIsNull {
    type Output;
    fn is_null(&self) -> Self::Output;
}

pub trait DaftNotNull {
    type Output;
    fn not_null(&self) -> Self::Output;
}

pub trait DaftIsNan {
    type Output;
    fn is_nan(&self) -> Self::Output;
}

pub trait DaftIsInf {
    type Output;
    fn is_inf(&self) -> Self::Output;
}

pub trait DaftNotNan {
    type Output;
    fn not_nan(&self) -> Self::Output;
}

pub trait DaftMinHash {
    type Output;
    fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        seed: u32,
        hasher: &impl BuildHasher,
    ) -> Self::Output;
}

pub type VecIndices = Vec<u64>;
pub type GroupIndices = Vec<VecIndices>;
pub type GroupIndicesPair = (VecIndices, GroupIndices);

pub trait IntoGroups {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair>;
}

pub trait IntoUniqueIdxs {
    fn make_unique_idxs(&self) -> DaftResult<VecIndices>;
}

pub trait DaftCountAggable {
    type Output;
    fn count(&self, mode: CountMode) -> Self::Output;
    fn grouped_count(&self, groups: &GroupIndices, mode: CountMode) -> Self::Output;
}

pub trait DaftApproxCountDistinctAggable {
    type Output;
    fn approx_count_distinct(&self) -> Self::Output;
    fn grouped_approx_count_distinct(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftSumAggable {
    type Output;
    fn sum(&self) -> Self::Output;
    fn grouped_sum(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftApproxSketchAggable {
    type Output;
    fn approx_sketch(&self) -> Self::Output;
    fn grouped_approx_sketch(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftMergeSketchAggable {
    type Output;
    fn merge_sketch(&self) -> Self::Output;
    fn grouped_merge_sketch(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftMeanAggable {
    type Output;
    fn mean(&self) -> Self::Output;
    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftStddevAggable {
    type Output;
    fn stddev(&self) -> Self::Output;
    fn grouped_stddev(&self, groups: &GroupIndices) -> Self::Output;
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

pub trait DaftHllSketchAggable {
    type Output;
    fn hll_sketch(&self) -> Self::Output;
    fn grouped_hll_sketch(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftHllMergeAggable {
    type Output;
    fn hll_merge(&self) -> Self::Output;
    fn grouped_hll_merge(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftSetAggable {
    type Output;
    fn set(&self) -> Self::Output;
    fn grouped_set(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftBoolAggable {
    type Output;
    fn bool_and(&self) -> Self::Output;
    fn bool_or(&self) -> Self::Output;
    fn grouped_bool_and(&self, groups: &GroupIndices) -> Self::Output;
    fn grouped_bool_or(&self, groups: &GroupIndices) -> Self::Output;
}

pub trait DaftSkewAggable {
    type Output;
    fn skew(&self) -> Self::Output;
    fn grouped_skew(&self, groups: &GroupIndices) -> Self::Output;
}
