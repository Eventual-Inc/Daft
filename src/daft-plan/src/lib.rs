mod builder;
mod display;
mod logical_plan;
mod ops;
mod source_info;

pub use builder::LogicalPlanBuilder;
pub use logical_plan::LogicalPlan;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<LogicalPlanBuilder>()?;

    Ok(())
}
