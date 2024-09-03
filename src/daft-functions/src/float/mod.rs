pub mod fill_nan;
pub mod is_inf;
pub mod is_nan;
pub mod not_nan;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(is_nan::py_is_nan))?;
    parent.add_wrapped(wrap_pyfunction!(is_inf::py_is_inf))?;
    parent.add_wrapped(wrap_pyfunction!(not_nan::py_not_nan))?;
    parent.add_wrapped(wrap_pyfunction!(fill_nan::py_fill_nan))?;

    Ok(())
}
