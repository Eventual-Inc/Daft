#![feature(hash_raw_entry)]
#![feature(async_closure)]
#[macro_use]
extern crate lazy_static;

mod array;
mod datatypes;
mod dsl;
mod io;
mod kernels;
mod schema;
mod series;
mod table;
mod utils;
#[cfg(feature = "python")]
mod ffi;
#[cfg(feature = "python")]
pub mod python;


pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_TYPE_DEV: &str = "dev";
pub const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};