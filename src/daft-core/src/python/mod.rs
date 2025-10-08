use pyo3::prelude::*;
pub mod series;

pub use daft_schema::python::{PyDataType, PyTimeUnit, field::PyField, schema::PySchema};
pub use series::PySeries;

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<series::PySeries>()?;
    parent.add_class::<series::PySeriesIterator>()?;
    daft_schema::python::register_modules(parent)?;
    Ok(())
}
