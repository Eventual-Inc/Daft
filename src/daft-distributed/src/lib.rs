#![feature(hash_map_macro)]

mod pipeline_node;
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod scheduling;
mod statistics;
pub(crate) mod utils;

#[cfg(feature = "python")]
pub use python::register_modules;
