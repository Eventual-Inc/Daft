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
pub mod file_format;
mod glob;
#[cfg(feature = "python")]
pub mod py_object_serde;

#[cfg(feature = "python")]
pub mod python;
pub mod storage_config;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
pub use python::register_modules;
use snafu::Snafu;
use storage_config::StorageConfig;

#[derive(Debug, Snafu)]
pub enum Error {
    #[cfg(feature = "python")]
    PyIO { source: PyErr },
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionField {
    field: Field,
    source_field: Option<Field>,
    transform: Option<Expr>,
}

impl PartitionField {
    pub fn new(
        field: Field,
        source_field: Option<Field>,
        transform: Option<Expr>,
    ) -> DaftResult<Self> {
        match (&source_field, &transform) {
            (Some(sf), Some(tfm)) => {
                let req_columns = get_required_columns(tfm);
                match req_columns.as_slice() {
                    [col] => {
                        if col == &sf.name {
                            Ok(PartitionField {
                                field,
                                source_field,
                                transform,
                            })
                        } else {
                            Err(DaftError::ValueError(format!("PartitionField transform's required column and source_field differ: {} vs {}" , col, sf.name)))
                        }
                    }
                    _ => Err(DaftError::ValueError(format!(
                        "PartitionField only supports unary transforms but received {}",
                        tfm
                    ))),
                }
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
            write!(f, "PartitionField({}, {})", self.field, tfm)
        } else {
            write!(f, "PartitionField({})", self.field)
        }
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
    pub filters: Option<Arc<Vec<ExprRef>>>,
    /// Optional columns to select from the source data.
    pub columns: Option<Arc<Vec<String>>>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
}

impl Default for Pushdowns {
    fn default() -> Self {
        Self::new(None, None, None)
    }
}

impl Pushdowns {
    pub fn new(
        filters: Option<Arc<Vec<ExprRef>>>,
        columns: Option<Arc<Vec<String>>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            filters,
            columns,
            limit,
        }
    }

    pub fn with_limit(&self, limit: Option<usize>) -> Self {
        Self {
            filters: self.filters.clone(),
            columns: self.columns.clone(),
            limit,
        }
    }

    pub fn with_filters(&self, filters: Option<Arc<Vec<ExprRef>>>) -> Self {
        Self {
            filters,
            columns: self.columns.clone(),
            limit: self.limit,
        }
    }

    pub fn with_columns(&self, columns: Option<Arc<Vec<String>>>) -> Self {
        Self {
            filters: self.filters.clone(),
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
            res.push(format!(
                "Filter pushdown = [{}]",
                filters
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = self.limit {
            res.push(format!("Limit pushdown = {}", limit));
        }
        res
    }
}
