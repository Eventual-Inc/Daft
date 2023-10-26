use std::{
    any::Any,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::SchemaRef};
use daft_dsl::{Expr, ExprRef};
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
pub use python::register_modules;
use storage_config::StorageConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataFileSource {
    AnonymousDataFile {
        path: String,
        metadata: Option<TableMetadata>,
        partition_spec: Option<PartitionSpec>,
        statistics: Option<TableStatistics>,
    },
    CatalogDataFile {
        path: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTask {
    // Micropartition will take this in as an input
    pub source: DataFileSource,
    pub file_format_config: Arc<FileFormatConfig>,
    pub schema: SchemaRef,
    pub storage_config: Arc<StorageConfig>,
    // TODO(Clark): Directly use the Pushdowns struct as part of the ScanTask struct?
    pub columns: Option<Arc<Vec<String>>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTaskBatch {
    pub sources: Vec<DataFileSource>,
    pub file_format_config: Arc<FileFormatConfig>,
    pub schema: SchemaRef,
    pub storage_config: Arc<StorageConfig>,
    // TODO(Clark): Directly use the Pushdowns struct as part of the ScanTaskBatch struct?
    pub columns: Option<Arc<Vec<String>>>,
    pub limit: Option<usize>,
    pub metadata: Option<TableMetadata>,
    pub statistics: Option<TableStatistics>,
}

impl ScanTaskBatch {
    pub fn new(
        sources: Vec<DataFileSource>,
        file_format_config: Arc<FileFormatConfig>,
        schema: SchemaRef,
        storage_config: Arc<StorageConfig>,
        columns: Option<Arc<Vec<String>>>,
        limit: Option<usize>,
    ) -> Self {
        assert!(!sources.is_empty());
        let (length, statistics) = sources
            .iter()
            .map(|s| {
                (
                    s.get_metadata().map(|m| m.length),
                    s.get_statistics().cloned(),
                )
            })
            .reduce(|(acc_len, acc_stats), (curr_len, curr_stats)| {
                (
                    acc_len.and_then(|acc_len| curr_len.map(|curr_len| acc_len + curr_len)),
                    acc_stats.and_then(|acc_stats| {
                        curr_stats.map(|curr_stats| acc_stats.union(&curr_stats).unwrap())
                    }),
                )
            })
            .unwrap();
        let metadata = length.map(|l| TableMetadata { length: l });
        Self {
            sources,
            file_format_config,
            schema,
            storage_config,
            columns,
            limit,
            metadata,
            statistics,
        }
    }

    pub fn num_rows(&self) -> Option<usize> {
        self.metadata.as_ref().map(|m| m.length)
    }

    pub fn size_bytes(&self) -> Option<usize> {
        self.statistics.as_ref().and_then(|s| {
            self.num_rows()
                .and_then(|num_rows| Some(num_rows * s.estimate_row_size().ok()?))
        })
    }
}

impl From<Vec<ScanTask>> for ScanTaskBatch {
    fn from(value: Vec<ScanTask>) -> Self {
        if value.is_empty() {
            panic!("Must have at least one ScanTask to create a ScanTaskBatch.");
        }
        let mut scan_task_iter = value.into_iter();
        let first_scan_task = scan_task_iter.next().unwrap();
        let first_scan_task_source = first_scan_task.source;
        let sources = vec![first_scan_task_source]
            .into_iter()
            .chain(scan_task_iter.map(|t| t.source))
            .collect::<Vec<_>>();
        Self::new(
            sources,
            first_scan_task.file_format_config,
            first_scan_task.schema,
            first_scan_task.storage_config,
            first_scan_task.columns,
            first_scan_task.limit,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionField {
    field: Field,
    source_field: Option<Field>,
    transform: Option<Expr>,
}

pub trait ScanOperator: Send + Sync + Display + Debug + DynHash {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[PartitionField];
    // fn statistics(&self) -> &TableStatistics;
    // fn clustering_spec(&self) -> &ClusteringSpec;

    fn can_absorb_filter(&self) -> bool;
    fn can_absorb_select(&self) -> bool;
    fn can_absorb_limit(&self) -> bool;
    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>>;
}

// The following implements PartialEq, Eq, and Hash for trait objects.

pub trait DynEq: Any {
    fn as_any(&self) -> &dyn Any;
    fn dyn_eq(&self, that: &dyn DynEq) -> bool;
}

impl<T: Any + PartialEq<Self>> DynEq for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn dyn_eq(&self, that: &dyn DynEq) -> bool {
        if let Some(that) = that.as_any().downcast_ref::<Self>() {
            self == that
        } else {
            false
        }
    }
}

pub trait DynHash: DynEq {
    fn dyn_hash(&self, hasher: &mut dyn Hasher);

    fn as_dyn_eq(&self) -> &dyn DynEq;
}

impl<H: Hash + DynEq> DynHash for H {
    fn dyn_hash(&self, mut hasher: &mut dyn Hasher) {
        H::hash(self, &mut hasher)
    }

    fn as_dyn_eq(&self) -> &dyn DynEq {
        self
    }
}

impl PartialEq for dyn ScanOperator + '_ {
    fn eq(&self, that: &dyn ScanOperator) -> bool {
        self.dyn_eq(that.as_dyn_eq())
    }
}

impl PartialEq<dyn ScanOperator> for Box<dyn ScanOperator + '_> {
    fn eq(&self, that: &dyn ScanOperator) -> bool {
        self.dyn_eq(that.as_dyn_eq())
    }
}

impl PartialEq<dyn ScanOperator> for Arc<dyn ScanOperator + '_> {
    fn eq(&self, that: &dyn ScanOperator) -> bool {
        self.dyn_eq(that.as_dyn_eq())
    }
}

impl Eq for dyn ScanOperator + '_ {}

impl Hash for dyn ScanOperator + '_ {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.dyn_hash(hasher)
    }
}

pub type ScanOperatorRef = Arc<dyn ScanOperator>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanExternalInfo {
    pub scan_op: Arc<dyn ScanOperator>,
    pub source_schema: SchemaRef,
    pub partitioning_keys: Vec<PartitionField>,
    pub pushdowns: Pushdowns,
}

impl ScanExternalInfo {
    pub fn new(
        scan_op: Arc<dyn ScanOperator>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
}
