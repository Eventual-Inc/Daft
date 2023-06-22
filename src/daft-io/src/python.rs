use crate::config::S3Config;
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pyclass]
struct PyS3Config {
    config: S3Config,
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyS3Config>()?;

    Ok(())
}
