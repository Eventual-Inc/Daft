mod builder;
mod display;
mod logical_plan;
mod ops;
mod source_info;

pub use builder::{new_plan_from_source, LogicalPlanBuilder};
pub use logical_plan::LogicalPlan;

#[cfg(feature = "python")]
pub mod pyapi;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pyapi::PyLogicalPlanBuilder>()?;

    parent.add_wrapped(wrap_pyfunction!(pyapi::source))?;
    Ok(())
}
