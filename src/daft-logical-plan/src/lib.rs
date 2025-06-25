#![feature(let_chains)]
#![feature(if_let_guard)]
#![feature(iterator_try_reduce)]

pub mod builder;
pub mod display;
pub mod logical_plan;
pub mod ops;
pub mod optimization;
pub mod partitioning;
pub mod sink_info;
pub mod source_info;
pub mod stats;
#[cfg(test)]
mod test;
mod treenode;

pub use builder::{LogicalPlanBuilder, PyLogicalPlanBuilder};
#[cfg(feature = "python")]
use common_file_formats::{
    python::PyFileFormatConfig, CsvSourceConfig, DatabaseSourceConfig, JsonSourceConfig,
    ParquetSourceConfig, WarcSourceConfig,
};
pub use daft_core::join::{JoinStrategy, JoinType};
pub use logical_plan::{LogicalPlan, LogicalPlanRef};
pub use ops::join::JoinOptions;
pub use partitioning::ClusteringSpec;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use sink_info::{
    CatalogType, DataSinkInfo, DeltaLakeCatalogInfo, IcebergCatalogInfo, LanceCatalogInfo,
};
pub use sink_info::{OutputFileInfo, SinkInfo};
pub use source_info::{InMemoryInfo, SourceInfo};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLogicalPlanBuilder>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<WarcSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<DatabaseSourceConfig>()?;
    parent.add_class::<JoinOptions>()?;

    Ok(())
}
