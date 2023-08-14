mod builder;
mod display;
mod join;
mod logical_plan;
mod ops;
mod partitioning;
mod physical_ops;
mod physical_plan;
mod planner;
mod resource_request;
mod sink_info;
mod source_info;

pub use builder::LogicalPlanBuilder;
pub use join::JoinType;
pub use logical_plan::LogicalPlan;
pub use partitioning::{PartitionScheme, PartitionSpec};
pub use physical_plan::PhysicalPlanScheduler;
pub use resource_request::ResourceRequest;
pub use source_info::{
    CsvSourceConfig, FileFormat, JsonSourceConfig, ParquetSourceConfig, PyFileFormatConfig,
};

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<LogicalPlanBuilder>()?;
    parent.add_class::<FileFormat>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<PartitionSpec>()?;
    parent.add_class::<PartitionScheme>()?;
    parent.add_class::<JoinType>()?;
    parent.add_class::<PhysicalPlanScheduler>()?;
    parent.add_class::<ResourceRequest>()?;

    Ok(())
}
