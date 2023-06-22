#![feature(hash_raw_entry)]

pub mod array;
pub mod datatypes;
mod dsl;
#[cfg(feature = "python")]
mod ffi;
mod kernels;
#[cfg(feature = "python")]
pub mod python;
mod schema;
pub mod series;
mod table;
mod utils;

pub use series::{Series, IntoSeries};
pub use datatypes::{DataType};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_TYPE_DEV: &str = "dev";
pub const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};
