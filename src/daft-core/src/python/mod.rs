use pyo3::prelude::*;
pub mod series;

pub use series::PySeries;

pub fn register_modules(py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<series::PySeries>()?;
    daft_schema::python::register_modules(py, parent)?;
    Ok(())
}
