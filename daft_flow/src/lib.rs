#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
fn daft_flow(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    daft_flow_pylib::register_modules(m)?;
    Ok(())
}
