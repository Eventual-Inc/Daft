#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(if_let_guard)]
#![feature(iterator_try_reduce)]

mod builder;
mod display;
mod logical_ops;
mod logical_optimization;
mod logical_plan;
mod partitioning;
mod physical_ops;
mod physical_plan;
mod physical_planner;
mod resource_request;
mod sink_info;
mod source_info;
#[cfg(test)]
mod test;
mod treenode;

pub use builder::{LogicalPlanBuilder, PyLogicalPlanBuilder};
pub use daft_core::join::{JoinStrategy, JoinType};
use daft_scan::file_format::FileFormat;
pub use logical_plan::{LogicalPlan, LogicalPlanRef};
pub use partitioning::ClusteringSpec;
pub use physical_plan::PhysicalPlanScheduler;
pub use resource_request::ResourceRequest;
pub use source_info::{FileInfo, FileInfos};

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use {
    daft_scan::file_format::{
        CsvSourceConfig, JsonSourceConfig, ParquetSourceConfig, PyFileFormatConfig,
    },
    daft_scan::storage_config::{NativeStorageConfig, PyStorageConfig, PythonStorageConfig},
};

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    use daft_scan::file_format::DatabaseSourceConfig;

    use crate::physical_planner::python::AdaptivePhysicalPlanScheduler;

    parent.add_class::<PyLogicalPlanBuilder>()?;
    parent.add_class::<FileFormat>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<DatabaseSourceConfig>()?;
    parent.add_class::<PhysicalPlanScheduler>()?;
    parent.add_class::<AdaptivePhysicalPlanScheduler>()?;
    parent.add_class::<ResourceRequest>()?;
    parent.add_class::<FileInfos>()?;
    parent.add_class::<FileInfo>()?;
    parent.add_class::<PyStorageConfig>()?;
    parent.add_class::<NativeStorageConfig>()?;
    parent.add_class::<PythonStorageConfig>()?;

    Ok(())
}
