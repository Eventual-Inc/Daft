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
mod slice;
mod sort;
mod sum;
mod value_counts;

pub use bool_and::{list_bool_and as bool_and, ListBoolAnd};
pub use bool_or::{list_bool_or as bool_or, ListBoolOr};
pub use chunk::{list_chunk as chunk, ListChunk};
pub use count::ListCount;
pub use count_distinct::{list_count_distinct as count_distinct, ListCountDistinct};
pub use distinct::{list_distinct as distinct, ListDistinct};
pub use explode::{explode, Explode};
pub use get::{list_get as get, ListGet};
pub use join::{list_join as join, ListJoin};
pub use list_fill::{list_fill, ListFill};
pub use max::{list_max as max, ListMax};
pub use mean::{list_mean as mean, ListMean};
pub use min::{list_min as min, ListMin};
pub use slice::{list_slice as slice, ListSlice};
pub use sort::{list_sort as sort, ListSort};
pub use sum::{list_sum as sum, ListSum};
pub use value_counts::{list_value_counts as value_counts, ListValueCounts};

pub(crate) mod kernels;
pub(crate) mod series;
pub use bool_and::*;
use daft_dsl::functions::FunctionModule;
pub use series::SeriesListExtension;

pub use crate::list_map::ListMap;
pub struct ListFunctions;

impl FunctionModule for ListFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
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
        parent.add_fn(ListSlice);
        parent.add_fn(ListSort);
        parent.add_fn(ListSum);
        parent.add_fn(ListValueCounts);
        parent.add_fn(ListMap);
    }
}
