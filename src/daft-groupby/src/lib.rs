//! Physical grouping helpers (`make_groups`, `make_unique_idxs`) for Daft arrays and series.

use daft_common_error::DaftResult;
use daft_core::array::ops::GroupIndices;
pub use daft_core::array::ops::VecIndices;

pub type Indices = Vec<u64>;
pub type GroupIndicesPair = (Indices, GroupIndices);

pub trait IntoGroups {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair>;
}

pub trait IntoUniqueIdxs {
    fn make_unique_idxs(&self) -> DaftResult<Indices>;
}

mod arrays;
mod series;
