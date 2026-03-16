#![feature(if_let_guard)]

pub mod builder;
pub mod display;
pub mod logical_plan;
pub mod ops;
pub mod optimization;
pub mod partitioning;
pub mod scan_builder;
pub mod sink_info;
pub mod source_info;
pub mod stats;
#[cfg(test)]
mod test;
mod treenode;

pub use builder::{LogicalPlanBuilder, PyLogicalPlanBuilder};
pub use daft_core::join::{JoinStrategy, JoinType};
#[cfg(feature = "python")]
use daft_scan::{
    CsvSourceConfig, DatabaseSourceConfig, JsonSourceConfig, ParquetSourceConfig, TextSourceConfig,
    WarcSourceConfig, python::PyFileFormatConfig,
};
pub use logical_plan::{LogicalPlan, LogicalPlanRef};
pub use ops::join::JoinOptions;
pub use partitioning::ClusteringSpec;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use sink_info::PyFormatSinkOption;
#[cfg(feature = "python")]
pub use sink_info::{
    CatalogType, DataSinkInfo, DeltaLakeCatalogInfo, IcebergCatalogInfo, LanceCatalogInfo,
};
pub use sink_info::{OutputFileInfo, SinkInfo};
pub use source_info::{InMemoryInfo, SourceInfo};

#[cfg(feature = "python")]
#[pyfunction]
pub fn logical_plan_table_scan(
    scan_operator: daft_scan::python::pylib::ScanOperatorHandle,
) -> PyResult<PyLogicalPlanBuilder> {
    Ok(LogicalPlanBuilder::table_scan(scan_operator.into(), None)?.into())
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLogicalPlanBuilder>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<PyFormatSinkOption>()?;
    parent.add_class::<ParquetSourceConfig>()?;
    parent.add_class::<JsonSourceConfig>()?;
    parent.add_class::<WarcSourceConfig>()?;
    parent.add_class::<TextSourceConfig>()?;
    parent.add_class::<CsvSourceConfig>()?;
    parent.add_class::<DatabaseSourceConfig>()?;
    parent.add_class::<JoinOptions>()?;
    parent.add_function(wrap_pyfunction!(logical_plan_table_scan, parent)?)?;

    Ok(())
}
