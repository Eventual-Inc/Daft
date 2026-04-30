#[cfg(feature = "python")]
mod python;
#[cfg(feature = "python")]
mod runners;

#[cfg(feature = "python")]
pub use runners::get_or_create_runner;

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::prelude::*;
    use pyo3::wrap_pyfunction;

    parent.add_function(wrap_pyfunction!(python::get_runner, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::get_or_create_runner, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::get_or_infer_runner_type, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_ray, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_native, parent)?)?;
    Ok(())
}
