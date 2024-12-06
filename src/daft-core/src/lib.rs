#![feature(let_chains)]
#![feature(int_roundings)]
#![feature(iterator_try_reduce)]
#![feature(if_let_guard)]
#![feature(hash_raw_entry)]

pub mod array;
pub mod count_mode;
pub mod datatypes;
pub mod join;
pub mod kernels;
#[cfg(feature = "python")]
pub mod python;
pub mod series;
pub mod utils;
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub mod prelude;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<count_mode::CountMode>()?;
    parent.add_class::<join::JoinType>()?;
    parent.add_class::<join::JoinStrategy>()?;
    parent.add_class::<join::JoinSide>()?;

    Ok(())
}
