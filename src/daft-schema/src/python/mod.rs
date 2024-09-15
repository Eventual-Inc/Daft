use pyo3::prelude::*;
pub mod datatype;
pub mod field;
pub mod schema;

use crate::image_format::ImageFormat;
use crate::image_mode::ImageMode;
pub use datatype::PyDataType;
pub use datatype::PyTimeUnit;

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<datatype::PyDataType>()?;
    parent.add_class::<datatype::PyTimeUnit>()?;
    parent.add_class::<schema::PySchema>()?;
    parent.add_class::<field::PyField>()?;
    parent.add_class::<ImageMode>()?;
    parent.add_class::<ImageFormat>()?;

    Ok(())
}
