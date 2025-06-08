use crate::functions::SQLFunctions;

pub mod aggs;
pub mod config;
pub mod map;
pub mod partitioning;
pub mod python;
pub mod sketch;
pub mod structs;
pub mod window;

pub use aggs::SQLModuleAggs;
pub use config::SQLModuleConfig;
pub use map::SQLModuleMap;
pub use partitioning::SQLModulePartitioning;
pub use python::SQLModulePython;
pub use sketch::SQLModuleSketch;
pub use structs::SQLModuleStructs;
pub use window::SQLModuleWindow;

/// A [SQLModule] is a collection of SQL functions that can be registered with a [SQLFunctions] instance.
///
/// This exists because we need to register Daft DSL functions with the SQL planner. We use an
/// approach similar to pyo3 modules as conceptually these solve the same problem – i.e. mapping
/// functions from one environment to another.
pub trait SQLModule {
    /// Register this module to the given [SQLFunctions] table.
    fn register(_parent: &mut SQLFunctions);
}
