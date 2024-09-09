#![feature(let_chains)]
#![feature(int_roundings)]
#![feature(iterator_try_reduce)]
#![feature(if_let_guard)]

pub mod array;
pub mod count_mode;
pub mod datatypes;
#[cfg(feature = "python")]
pub mod ffi;
pub mod join;
pub mod kernels;
#[cfg(feature = "python")]
pub mod python;
pub mod schema;
pub mod series;
pub mod utils;
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub mod prelude;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_TYPE_DEV: &str = "dev";
pub const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<count_mode::CountMode>()?;
    parent.add_class::<join::JoinType>()?;
    parent.add_class::<join::JoinStrategy>()?;

    Ok(())
}
