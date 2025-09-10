#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(feature = "python")]
use pyo3::{Bound, PyResult};

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::types::PyModuleMethods;
    use pyo3::wrap_pyfunction;

    parent.add_function(wrap_pyfunction!(python::set_runner_ray, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_native, parent)?)?;
    Ok(())
}
