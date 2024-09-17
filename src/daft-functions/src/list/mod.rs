mod chunk;
mod count;
mod explode;
mod get;
mod join;
mod max;
mod mean;
mod min;
mod slice;
mod sum;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(explode::py_explode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(chunk::py_list_chunk, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(count::py_list_count, parent)?)?;
    Ok(())
}
