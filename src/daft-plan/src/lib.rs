#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(if_let_guard)]
#![feature(iterator_try_reduce)]

mod builder;
pub mod display;
mod logical_ops;
mod logical_optimization;
pub mod logical_plan;
mod partitioning;
pub mod physical_ops;
mod physical_optimization;
mod physical_plan;
mod physical_planner;
mod sink_info;
pub mod source_info;
#[cfg(test)]
mod test;
mod treenode;

pub use builder::{LogicalPlanBuilder, ParquetScanBuilder, PyLogicalPlanBuilder};
pub use daft_core::join::{JoinStrategy, JoinType};
pub use logical_plan::{LogicalPlan, LogicalPlanRef};
pub use partitioning::ClusteringSpec;
pub use physical_plan::{PhysicalPlan, PhysicalPlanRef};
pub use physical_planner::{
    logical_to_physical, populate_aggregation_stages, AdaptivePlanner, MaterializedResults,
    QueryStageOutput,
};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use sink_info::{DeltaLakeCatalogInfo, IcebergCatalogInfo, LanceCatalogInfo};
pub use sink_info::{OutputFileInfo, SinkInfo};
pub use source_info::{FileInfo, FileInfos, InMemoryInfo, SourceInfo};
#[cfg(feature = "python")]
use {
    common_file_formats::{
        python::PyFileFormatConfig, CsvSourceConfig, DatabaseSourceConfig, JsonSourceConfig,
        ParquetSourceConfig,
    },
    daft_scan::storage_config::{NativeStorageConfig, PyStorageConfig, PythonStorageConfig},
};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLogicalPlanBuilder>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<DatabaseSourceConfig>()?;
    parent.add_class::<FileInfos>()?;
    parent.add_class::<FileInfo>()?;
    parent.add_class::<PyStorageConfig>()?;
    parent.add_class::<NativeStorageConfig>()?;
    parent.add_class::<PythonStorageConfig>()?;

    Ok(())
}
