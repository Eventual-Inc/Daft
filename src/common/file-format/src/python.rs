use pyo3::prelude::*;

use crate::FileFormat;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<FileFormat>()?;
    Ok(())
}
