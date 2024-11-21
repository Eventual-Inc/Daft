use pyo3::prelude::*;
pub mod datatype;
pub mod field;
pub mod schema;

pub use datatype::{PyDataType, PyTimeUnit};

use crate::{image_format::ImageFormat, image_mode::ImageMode};

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<datatype::PyDataType>()?;
    parent.add_class::<datatype::PyTimeUnit>()?;
    parent.add_class::<schema::PySchema>()?;
    parent.add_class::<field::PyField>()?;
    parent.add_class::<ImageMode>()?;
    parent.add_class::<ImageFormat>()?;

    Ok(())
}
