//! Physical grouping helpers (`make_groups`, `make_unique_idxs`) for Daft arrays and series.

use common_error::DaftResult;
pub use daft_core::array::ops::{GroupIndicesPair, VecIndices};

pub trait IntoGroups {
    fn make_groups(&self) -> DaftResult<GroupIndicesPair>;
}

pub trait IntoUniqueIdxs {
    fn make_unique_idxs(&self) -> DaftResult<VecIndices>;
}

mod arrays;
mod series;
