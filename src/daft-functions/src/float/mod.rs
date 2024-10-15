mod fill_nan;
mod is_inf;
mod is_nan;
mod not_nan;

pub use fill_nan::fill_nan;
pub use is_inf::is_inf;
pub use is_nan::is_nan;
pub use not_nan::not_nan;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(fill_nan::py_fill_nan, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(is_inf::py_is_inf, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(is_nan::py_is_nan, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(not_nan::py_not_nan, parent)?)?;

    Ok(())
}
