#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(if_let_guard)]

mod builder;
mod display;
mod join;
mod logical_ops;
mod logical_plan;
mod optimization;
mod partitioning;
mod physical_ops;
mod physical_plan;
mod planner;
mod resource_request;
mod sink_info;
mod source_info;
#[cfg(test)]
mod test;

pub use builder::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_scan::{
    file_format::{
        CsvSourceConfig, FileFormat, JsonSourceConfig, ParquetSourceConfig, PyFileFormatConfig,
    },
    storage_config::{NativeStorageConfig, PyStorageConfig},
};
pub use join::JoinType;
pub use logical_plan::LogicalPlan;
pub use partitioning::{PartitionScheme, PartitionSpec};
pub use physical_plan::PhysicalPlanScheduler;
pub use resource_request::ResourceRequest;
pub use source_info::{FileInfo, FileInfos};

#[cfg(feature = "python")]
use daft_scan::storage_config::PythonStorageConfig;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyLogicalPlanBuilder>()?;
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
    parent.add_class::<FileInfos>()?;
    parent.add_class::<FileInfo>()?;
    parent.add_class::<PyStorageConfig>()?;
    parent.add_class::<NativeStorageConfig>()?;
    parent.add_class::<PythonStorageConfig>()?;

    Ok(())
}
