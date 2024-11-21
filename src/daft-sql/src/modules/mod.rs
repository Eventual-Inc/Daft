use crate::functions::SQLFunctions;

pub mod aggs;
pub mod config;
pub mod float;
pub mod hashing;
pub mod image;
pub mod json;
pub mod list;
pub mod map;
pub mod numeric;
pub mod partitioning;
pub mod python;
pub mod sketch;
pub mod structs;
pub mod temporal;
pub mod utf8;

pub use aggs::SQLModuleAggs;
pub use config::SQLModuleConfig;
pub use float::SQLModuleFloat;
pub use image::SQLModuleImage;
pub use json::SQLModuleJson;
pub use list::SQLModuleList;
pub use map::SQLModuleMap;
pub use numeric::SQLModuleNumeric;
pub use partitioning::SQLModulePartitioning;
pub use python::SQLModulePython;
pub use sketch::SQLModuleSketch;
pub use structs::SQLModuleStructs;
pub use temporal::SQLModuleTemporal;
pub use utf8::SQLModuleUtf8;

/// A [SQLModule] is a collection of SQL functions that can be registered with a [SQLFunctions] instance.
///
/// This exists because we need to register Daft DSL functions with the SQL planner. We use an
/// approach similar to pyo3 modules as conceptually these solve the same problem – i.e. mapping
/// functions from one environment to another.
pub trait SQLModule {
    /// Register this module to the given [SQLFunctions] table.
    fn register(_parent: &mut SQLFunctions);
}
