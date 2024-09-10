use pyo3::prelude::*;
pub mod series;

pub use series::PySeries;

pub use daft_schema::python::{field::PyField, schema::PySchema, PyDataType, PyTimeUnit};

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<series::PySeries>()?;
    daft_schema::python::register_modules(parent)?;
    Ok(())
}
