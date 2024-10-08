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
pub(crate) mod broadcast;
pub(crate) mod cast;
mod cbrt;
mod ceil;
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
mod list;
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
mod sort;
pub(crate) mod sparse_tensor;
mod sqrt;
mod struct_;
mod sum;
mod take;
pub(crate) mod tensor;
mod time;
pub mod trigonometry;
mod truncate;
mod utf8;

use common_error::DaftResult;
pub use hll_sketch::HLL_SKETCH_DTYPE;
pub use sort::{build_multi_array_bicompare, build_multi_array_compare};
pub use utf8::{PadPlacement, Utf8NormalizeOptions};

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
    fn minhash(&self, num_hashes: usize, ngram_size: usize, seed: u32) -> Self::Output;
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

pub trait DaftCompareAggable {
    type Output;
    fn min(&self) -> Self::Output;
    fn max(&self) -> Self::Output;
    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output;
    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output;
}

/// Trait for types that can be aggregated into list-like structures.
pub trait DaftListAggable {
    /// The output type of the list aggregation operations.
    type Output;

    /// Converts the current data into a list-like structure.
    fn list(&self) -> Self::Output;

    /// Groups elements of the current data structure according to the provided group indices.
    ///
    /// This method creates a new list-like structure where each group of indices from the input
    /// becomes a single element in the output. The grouping is defined by `GroupIndices`, which
    /// is a vector of index vectors.
    ///
    /// # Arguments
    ///
    /// * `groups` - A reference to `GroupIndices`, which is a `Vec<Vec<u64>>`. Each inner `Vec<u64>`
    ///   represents a group of indices that should be combined into a single element in the output.
    ///
    /// # Returns
    ///
    /// Returns a new instance of `Self::Output`, which is a list-like structure where each element
    /// corresponds to a group from the input `groups`.
    ///
    /// # Example
    ///
    /// Suppose we have an array of integers: `[10, 20, 30, 40, 50]`
    /// And we call `grouped_list` with the following groups:
    ///
    /// ```
    /// use daft_core::array::ops::GroupIndices;
    /// let groups: GroupIndices = vec![
    ///     vec![0, 2],    // Group 1: indices 0 and 2
    ///     vec![1, 3, 4]  // Group 2: indices 1, 3, and 4
    /// ];
    /// ```
    ///
    /// The resulting output would be a list-like structure conceptually represented as:
    /// `[[10, 30], [20, 40, 50]]`
    ///
    /// Where:
    /// - The first element `[10, 30]` corresponds to group 1 (indices 0 and 2)
    /// - The second element `[20, 40, 50]` corresponds to group 2 (indices 1, 3, and 4)
    ///
    /// The actual representation may vary depending on the implementing type.
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
