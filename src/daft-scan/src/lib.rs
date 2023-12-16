#![feature(if_let_guard)]
#![feature(let_chains)]
use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::{optimization::get_required_columns, Expr, ExprRef};
use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
use file_format::FileFormatConfig;
use serde::{Deserialize, Serialize};

mod anonymous;
pub use anonymous::AnonymousScanOperator;
pub mod file_format;
mod glob;
#[cfg(feature = "python")]
pub mod py_object_serde;
pub mod scan_task_iters;

#[cfg(feature = "python")]
pub mod python;
pub mod storage_config;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
pub use python::register_modules;
use snafu::Snafu;
use storage_config::StorageConfig;
mod expr_rewriter;
pub use expr_rewriter::rewrite_predicate_for_partitioning;
#[derive(Debug, Snafu)]
pub enum Error {
    #[cfg(feature = "python")]
    PyIO { source: PyErr },

    #[snafu(display(
        "PartitionSpecs were different during ScanTask::merge: {:?} vs {:?}",
        ps1,
        ps2
    ))]
    DifferingPartitionSpecsInScanTaskMerge {
        ps1: Option<PartitionSpec>,
        ps2: Option<PartitionSpec>,
    },

    #[snafu(display("Schemas were different during ScanTask::merge: {} vs {}", s1, s2))]
    DifferingSchemasInScanTaskMerge { s1: SchemaRef, s2: SchemaRef },

    #[snafu(display(
        "FileFormatConfigs were different during ScanTask::merge: {:?} vs {:?}",
        ffc1,
        ffc2
    ))]
    DifferingFileFormatConfigsInScanTaskMerge {
        ffc1: Arc<FileFormatConfig>,
        ffc2: Arc<FileFormatConfig>,
    },

    #[snafu(display(
        "StorageConfigs were different during ScanTask::merge: {:?} vs {:?}",
        sc1,
        sc2
    ))]
    DifferingStorageConfigsInScanTaskMerge {
        sc1: Arc<StorageConfig>,
        sc2: Arc<StorageConfig>,
    },

    #[snafu(display(
        "Pushdowns were different during ScanTask::merge: {:?} vs {:?}",
        p1,
        p2
    ))]
    DifferingPushdownsInScanTaskMerge { p1: Pushdowns, p2: Pushdowns },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        DaftError::External(value.into())
    }
}

#[cfg(feature = "python")]
impl From<Error> for pyo3::PyErr {
    fn from(value: Error) -> Self {
        let daft_error: DaftError = value.into();
        daft_error.into()
    }
}

/// Specification of a subset of a file to be read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkSpec {
    /// Selection of Parquet row groups.
    Parquet(Vec<i64>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataFileSource {
    AnonymousDataFile {
        path: String,
        chunk_spec: Option<ChunkSpec>,
        size_bytes: Option<u64>,
        metadata: Option<TableMetadata>,
        partition_spec: Option<PartitionSpec>,
        statistics: Option<TableStatistics>,
    },
    CatalogDataFile {
        path: String,
        chunk_spec: Option<ChunkSpec>,
        size_bytes: Option<u64>,
        metadata: TableMetadata,
        partition_spec: PartitionSpec,
        statistics: Option<TableStatistics>,
    },
}

impl DataFileSource {
    pub fn get_path(&self) -> &str {
        match self {
            Self::AnonymousDataFile { path, .. } | Self::CatalogDataFile { path, .. } => path,
        }
    }

    pub fn get_chunk_spec(&self) -> Option<&ChunkSpec> {
        match self {
            Self::AnonymousDataFile { chunk_spec, .. }
            | Self::CatalogDataFile { chunk_spec, .. } => chunk_spec.as_ref(),
        }
    }

    pub fn get_size_bytes(&self) -> Option<u64> {
        match self {
            Self::AnonymousDataFile { size_bytes, .. }
            | Self::CatalogDataFile { size_bytes, .. } => *size_bytes,
        }
    }

    pub fn get_metadata(&self) -> Option<&TableMetadata> {
        match self {
            Self::AnonymousDataFile { metadata, .. } => metadata.as_ref(),
            Self::CatalogDataFile { metadata, .. } => Some(metadata),
        }
    }

    pub fn get_statistics(&self) -> Option<&TableStatistics> {
        match self {
            Self::AnonymousDataFile { statistics, .. }
            | Self::CatalogDataFile { statistics, .. } => statistics.as_ref(),
        }
    }

    pub fn get_partition_spec(&self) -> Option<&PartitionSpec> {
        match self {
            Self::AnonymousDataFile { partition_spec, .. } => partition_spec.as_ref(),
            Self::CatalogDataFile { partition_spec, .. } => Some(partition_spec),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScanTask {
    pub sources: Vec<DataFileSource>,
    pub file_format_config: Arc<FileFormatConfig>,
    pub schema: SchemaRef,
    pub storage_config: Arc<StorageConfig>,
    pub pushdowns: Pushdowns,
    pub size_bytes_on_disk: Option<u64>,
    pub metadata: Option<TableMetadata>,
    pub statistics: Option<TableStatistics>,
}
pub type ScanTaskRef = Arc<ScanTask>;

impl ScanTask {
    pub fn new(
        sources: Vec<DataFileSource>,
        file_format_config: Arc<FileFormatConfig>,
        schema: SchemaRef,
        storage_config: Arc<StorageConfig>,
        pushdowns: Pushdowns,
    ) -> Self {
        assert!(!sources.is_empty());
        debug_assert!(
            sources
                .iter()
                .all(|s| s.get_partition_spec() == sources.first().unwrap().get_partition_spec()),
            "ScanTask sources must all have the same PartitionSpec at construction",
        );
        let (length, size_bytes_on_disk, statistics) = sources
            .iter()
            .map(|s| {
                (
                    s.get_metadata().map(|m| m.length),
                    s.get_size_bytes(),
                    s.get_statistics().cloned(),
                )
            })
            .reduce(
                |(acc_len, acc_size, acc_stats), (curr_len, curr_size, curr_stats)| {
                    (
                        acc_len.and_then(|acc_len| curr_len.map(|curr_len| acc_len + curr_len)),
                        acc_size
                            .and_then(|acc_size| curr_size.map(|curr_size| acc_size + curr_size)),
                        acc_stats.and_then(|acc_stats| {
                            curr_stats.map(|curr_stats| acc_stats.union(&curr_stats).unwrap())
                        }),
                    )
                },
            )
            .unwrap();
        let metadata = length.map(|l| TableMetadata { length: l });
        Self {
            sources,
            file_format_config,
            schema,
            storage_config,
            pushdowns,
            size_bytes_on_disk,
            metadata,
            statistics,
        }
    }

    pub fn merge(sc1: &ScanTask, sc2: &ScanTask) -> Result<ScanTask, Error> {
        if sc1.partition_spec() != sc2.partition_spec() {
            return Err(Error::DifferingPartitionSpecsInScanTaskMerge {
                ps1: sc1.partition_spec().cloned(),
                ps2: sc2.partition_spec().cloned(),
            });
        }
        if sc1.file_format_config != sc2.file_format_config {
            return Err(Error::DifferingFileFormatConfigsInScanTaskMerge {
                ffc1: sc1.file_format_config.clone(),
                ffc2: sc2.file_format_config.clone(),
            });
        }
        if sc1.schema != sc2.schema {
            return Err(Error::DifferingSchemasInScanTaskMerge {
                s1: sc1.schema.clone(),
                s2: sc2.schema.clone(),
            });
        }
        if sc1.storage_config != sc2.storage_config {
            return Err(Error::DifferingStorageConfigsInScanTaskMerge {
                sc1: sc1.storage_config.clone(),
                sc2: sc2.storage_config.clone(),
            });
        }
        if sc1.pushdowns != sc2.pushdowns {
            return Err(Error::DifferingPushdownsInScanTaskMerge {
                p1: sc1.pushdowns.clone(),
                p2: sc2.pushdowns.clone(),
            });
        }
        Ok(ScanTask::new(
            sc1.sources
                .clone()
                .into_iter()
                .chain(sc2.sources.clone())
                .collect(),
            sc1.file_format_config.clone(),
            sc1.schema.clone(),
            sc1.storage_config.clone(),
            sc1.pushdowns.clone(),
        ))
    }

    pub fn num_rows(&self) -> Option<usize> {
        self.metadata.as_ref().map(|m| m.length)
    }

    pub fn size_bytes(&self) -> Option<usize> {
        self.statistics
            .as_ref()
            .and_then(|s| {
                // Derive in-memory size estimate from table stats.
                self.num_rows()
                    .and_then(|num_rows| Some(num_rows * s.estimate_row_size().ok()?))
            })
            // Fall back on on-disk size.
            .or_else(|| self.size_bytes_on_disk.map(|s| s as usize))
    }

    pub fn partition_spec(&self) -> Option<&PartitionSpec> {
        match self.sources.first() {
            None => None,
            Some(source) => source.get_partition_spec(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionField {
    field: Field,
    source_field: Option<Field>,
    transform: Option<PartitionTransform>,
}

impl PartitionField {
    pub fn new(
        field: Field,
        source_field: Option<Field>,
        transform: Option<PartitionTransform>,
    ) -> DaftResult<Self> {
        match (&source_field, &transform) {
            (Some(_), Some(_)) => {
                // TODO ADD VALIDATION OF TRANSFORM based on types
                Ok(PartitionField {
                    field,
                    source_field,
                    transform,
                })
            }
            (None, Some(tfm)) => Err(DaftError::ValueError(format!(
                "transform set in PartitionField: {} but source_field not set",
                tfm
            ))),
            _ => Ok(PartitionField {
                field,
                source_field,
                transform,
            }),
        }
    }
}

impl Display for PartitionField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(tfm) = &self.transform {
            write!(
                f,
                "PartitionField({}, src={}, tfm={})",
                self.field,
                self.source_field.as_ref().unwrap(),
                tfm
            )
        } else {
            write!(f, "PartitionField({})", self.field)
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PartitionTransform {
    /// https://iceberg.apache.org/spec/#partitioning
    /// For Delta, Hudi and Hive, it should always be `Identity`.
    Identity,
    Bucket(u64),
    Truncate(u64),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

impl PartitionTransform {
    pub fn supports_equals(&self) -> bool {
        true
    }

    pub fn supports_not_equals(&self) -> bool {
        matches!(self, Self::Identity)
    }

    pub fn supports_comparison(&self) -> bool {
        use PartitionTransform::*;
        matches!(self, Identity | Truncate(_) | Year | Month | Day | Hour)
    }
}

impl Display for PartitionTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub trait ScanOperator: Send + Sync + Display + Debug {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[PartitionField];

    fn can_absorb_filter(&self) -> bool;
    fn can_absorb_select(&self) -> bool;
    fn can_absorb_limit(&self) -> bool;
    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>>;
}

/// Light transparent wrapper around an Arc<dyn ScanOperator> that implements Eq/PartialEq/Hash
/// functionality to be performed on the **pointer** instead of on the value in the pointer.
///
/// This lets us get around having to implement full hashing/equality on [`ScanOperator`]`, which
/// is difficult because we sometimes have weird Python implementations that can be hard to check.
///
/// [`ScanOperatorRef`] should be thus held by structs that need to check the "sameness" of the
/// underlying ScanOperator instance, for example in the Scan nodes in a logical plan which need
/// to check for sameness of Scan nodes during plan optimization.
#[derive(Debug, Clone)]
pub struct ScanOperatorRef(pub Arc<dyn ScanOperator>);

impl Hash for ScanOperatorRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state)
    }
}

impl PartialEq<ScanOperatorRef> for ScanOperatorRef {
    fn eq(&self, other: &ScanOperatorRef) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::cmp::Eq for ScanOperatorRef {}

impl Display for ScanOperatorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanExternalInfo {
    pub scan_op: ScanOperatorRef,
    pub source_schema: SchemaRef,
    pub partitioning_keys: Vec<PartitionField>,
    pub pushdowns: Pushdowns,
}

impl ScanExternalInfo {
    pub fn new(
        scan_op: ScanOperatorRef,
        source_schema: SchemaRef,
        partitioning_keys: Vec<PartitionField>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            scan_op,
            source_schema,
            partitioning_keys,
            pushdowns,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Pushdowns {
    /// Optional filters to apply to the source data.
    pub filters: Option<ExprRef>,
    /// Optional filters to apply on partitioning keys.
    pub partition_filters: Option<ExprRef>,
    /// Optional columns to select from the source data.
    pub columns: Option<Arc<Vec<String>>>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
}

impl Default for Pushdowns {
    fn default() -> Self {
        Self::new(None, None, None, None)
    }
}

impl Pushdowns {
    pub fn new(
        filters: Option<ExprRef>,
        partition_filters: Option<ExprRef>,
        columns: Option<Arc<Vec<String>>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            filters,
            partition_filters,
            columns,
            limit,
        }
    }

    pub fn with_limit(&self, limit: Option<usize>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit,
        }
    }

    pub fn with_filters(&self, filters: Option<ExprRef>) -> Self {
        Self {
            filters,
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
        }
    }

    pub fn with_partition_filters(&self, partition_filters: Option<ExprRef>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters,
            columns: self.columns.clone(),
            limit: self.limit,
        }
    }

    pub fn with_columns(&self, columns: Option<Arc<Vec<String>>>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns,
            limit: self.limit,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(columns) = &self.columns {
            res.push(format!("Projection pushdown = [{}]", columns.join(", ")));
        }
        if let Some(filters) = &self.filters {
            res.push(format!("Filter pushdown = {}", filters));
        }
        if let Some(pfilters) = &self.partition_filters {
            res.push(format!("Partition Filter = {}", pfilters));
        }
        if let Some(limit) = self.limit {
            res.push(format!("Limit pushdown = {}", limit));
        }
        res
    }
}
