#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(if_let_guard)]
#![feature(iterator_try_reduce)]

mod builder;
mod display;
mod logical_ops;
mod logical_optimization;
pub mod logical_plan;
mod partitioning;
pub mod physical_ops;
mod physical_plan;
mod physical_planner;
mod resource_request;
mod sink_info;
pub mod source_info;
#[cfg(test)]
mod test;
mod treenode;

pub use builder::{LogicalPlanBuilder, PyLogicalPlanBuilder};
pub use daft_core::join::{JoinStrategy, JoinType};
use daft_scan::file_format::FileFormat;
pub use logical_plan::{LogicalPlan, LogicalPlanRef};
pub use partitioning::ClusteringSpec;
pub use physical_plan::{PhysicalPlan, PhysicalPlanRef};
pub use physical_planner::{
    logical_to_physical, populate_aggregation_stages, AdaptivePlanner, MaterializedResults,
    QueryStageOutput,
};
pub use resource_request::ResourceRequest;
pub use sink_info::{OutputFileInfo, SinkInfo};
pub use source_info::{FileInfo, FileInfos, InMemoryInfo, SourceInfo};

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use sink_info::{DeltaLakeCatalogInfo, IcebergCatalogInfo, LanceCatalogInfo};
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

    parent.add_class::<PyLogicalPlanBuilder>()?;
    parent.add_class::<FileFormat>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<DatabaseSourceConfig>()?;
    parent.add_class::<ResourceRequest>()?;
    parent.add_class::<FileInfos>()?;
    parent.add_class::<FileInfo>()?;
    parent.add_class::<PyStorageConfig>()?;
    parent.add_class::<NativeStorageConfig>()?;
    parent.add_class::<PythonStorageConfig>()?;

    Ok(())
}
