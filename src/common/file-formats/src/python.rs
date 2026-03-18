use pyo3::prelude::*;

use crate::{FileFormat, WriteMode};

#[pymethods]
impl WriteMode {
    #[staticmethod]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(mode: &str) -> PyResult<Self> {
        Ok(mode.parse()?)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<FileFormat>()?;
    parent.add_class::<WriteMode>()?;
    Ok(())
}
