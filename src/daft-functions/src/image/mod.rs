pub mod decode;
pub mod encode;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(decode::py_decode))?;
    parent.add_wrapped(wrap_pyfunction!(encode::py_encode))?;

    Ok(())
}
