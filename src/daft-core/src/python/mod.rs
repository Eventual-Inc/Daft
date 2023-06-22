use pyo3::prelude::*;
pub mod datatype;
pub mod field;
pub mod schema;
pub mod series;

use crate::datatypes::ImageFormat;
use crate::datatypes::ImageMode;
pub use datatype::PyDataType;
pub use series::PySeries;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<series::PySeries>()?;
    parent.add_class::<datatype::PyDataType>()?;
    parent.add_class::<datatype::PyTimeUnit>()?;
    parent.add_class::<schema::PySchema>()?;
    parent.add_class::<field::PyField>()?;
    parent.add_class::<ImageMode>()?;
    parent.add_class::<ImageFormat>()?;

    Ok(())
}
