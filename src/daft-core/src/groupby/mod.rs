use daft_common::error::DaftResult;
use crate::array::ops::GroupIndices;
pub use crate::array::ops::VecIndices;

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
