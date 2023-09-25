#![feature(let_chains)]

pub mod array;
pub mod count_mode;
pub mod datatypes;
#[cfg(feature = "python")]
pub mod ffi;
pub mod kernels;
#[cfg(feature = "python")]
pub mod python;
pub mod schema;
pub mod series;
pub mod utils;
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub use count_mode::CountMode;
pub use datatypes::DataType;
pub use series::{IntoSeries, Series};

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<CountMode>()?;

    Ok(())
}
