mod append;
mod bool_and;
mod bool_or;
mod chunk;
mod count;
mod count_distinct;
mod distinct;
mod explode;
mod get;
mod join;
mod list_fill;
mod list_map;
mod max;
mod mean;
mod min;
mod sort;
mod sum;
mod value_counts;

pub use append::{ListAppend, list_append as append};
pub use bool_and::{ListBoolAnd, list_bool_and as bool_and};
pub use bool_or::{ListBoolOr, list_bool_or as bool_or};
pub use chunk::{ListChunk, list_chunk as chunk};
pub use count::ListCount;
pub use count_distinct::{ListCountDistinct, list_count_distinct as count_distinct};
pub use distinct::{ListDistinct, list_distinct as distinct};
pub use explode::{Explode, explode};
pub use get::{ListGet, list_get as get};
pub use join::{ListJoin, list_join as join};
pub use list_fill::{ListFill, list_fill};
pub use max::{ListMax, list_max as max};
pub use mean::{ListMean, list_mean as mean};
pub use min::{ListMin, list_min as min};
pub use sort::{ListSort, list_sort as sort};
pub use sum::{ListSum, list_sum as sum};
pub use value_counts::{ListValueCounts, list_value_counts as value_counts};

pub(crate) mod kernels;
pub(crate) mod series;
pub use bool_and::*;
use daft_dsl::functions::FunctionModule;
pub use series::SeriesListExtension;

pub use crate::list_map::ListMap;
pub struct ListFunctions;

impl FunctionModule for ListFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(ListAppend);
        parent.add_fn(ListBoolAnd);
        parent.add_fn(ListBoolOr);
        parent.add_fn(ListChunk);
        parent.add_fn(ListCount);
        parent.add_fn(ListCountDistinct);
        parent.add_fn(ListDistinct);
        parent.add_fn(Explode);
        parent.add_fn(ListGet);
        parent.add_fn(ListJoin);
        parent.add_fn(ListFill);
        parent.add_fn(ListMax);
        parent.add_fn(ListMean);
        parent.add_fn(ListMin);
        parent.add_fn(ListSort);
        parent.add_fn(ListSum);
        parent.add_fn(ListValueCounts);
        parent.add_fn(ListMap);
    }
}
