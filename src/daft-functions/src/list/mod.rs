mod chunk;
mod count;
mod explode;
mod get;
mod join;
mod max;
mod mean;
mod min;
mod slice;
mod sort;
mod sum;
mod value_counts;

pub use chunk::{list_chunk as chunk, ListChunk};
pub use count::{list_count as count, ListCount};
pub use explode::{explode, Explode};
pub use get::{list_get as get, ListGet};
pub use join::{list_join as join, ListJoin};
pub use max::{list_max as max, ListMax};
pub use mean::{list_mean as mean, ListMean};
pub use min::{list_min as min, ListMin};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use slice::{list_slice as slice, ListSlice};
pub use sort::{list_sort as sort, ListSort};
pub use sum::{list_sum as sum, ListSum};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(explode::py_explode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(chunk::py_list_chunk, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(count::py_list_count, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(get::py_list_get, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(join::py_list_join, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        value_counts::py_list_value_counts,
        parent
    )?)?;

    parent.add_function(wrap_pyfunction_bound!(max::py_list_max, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(min::py_list_min, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(mean::py_list_mean, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(min::py_list_min, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(slice::py_list_slice, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(sum::py_list_sum, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(sort::py_list_sort, parent)?)?;

    Ok(())
}
