#![feature(once_cell_try)]

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(feature = "python")]
use pyo3::{Bound, PyResult};

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "python")]
mod runners;

#[cfg(feature = "python")]
pub use runners::get_or_create_runner;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::wrap_pyfunction;

    parent.add_function(wrap_pyfunction!(python::get_runner, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::get_or_create_runner, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::get_or_infer_runner_type, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_ray, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_native, parent)?)?;
    Ok(())
}
