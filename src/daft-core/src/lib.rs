#![feature(let_chains)]

pub mod array;
pub mod datatypes;
#[cfg(feature = "python")]
pub mod ffi;
pub mod kernels;
#[cfg(feature = "python")]
pub mod python;
pub mod schema;
pub mod series;
pub mod utils;

pub use datatypes::DataType;
pub use series::{IntoSeries, Series};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_TYPE_DEV: &str = "dev";
pub const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};
