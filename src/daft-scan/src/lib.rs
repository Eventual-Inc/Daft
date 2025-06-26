#![feature(if_let_guard)]
#![feature(let_chains)]
use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

use common_display::DisplayAs;
use common_error::DaftError;
use common_file_formats::FileFormatConfig;
use common_scan_info::{Pushdowns, ScanTaskLike, ScanTaskLikeRef};
use daft_schema::schema::{Schema, SchemaRef};
use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
use itertools::Itertools;
use parquet2::metadata::FileMetaData;
use serde::{Deserialize, Serialize};

mod anonymous;
pub use anonymous::AnonymousScanOperator;
pub mod glob;
mod hive;
use common_daft_config::DaftExecutionConfig;
pub mod builder;
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
        "FilePathColumns were different during ScanTask::merge: {:?} vs {:?}",
        fpc1,
        fpc2
    ))]
    DifferingGeneratedFieldsInScanTaskMerge {
        fpc1: Option<SchemaRef>,
        fpc2: Option<SchemaRef>,
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
        Self::External(value.into())
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ChunkSpec {
    /// Selection of Parquet row groups.
    Parquet(Vec<i64>),
}

impl ChunkSpec {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        match self {
            Self::Parquet(chunks) => {
                res.push(format!("Chunks = {chunks:?}"));
            }
        }
        res
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataSource {
    File {
        path: String,
        chunk_spec: Option<ChunkSpec>,
        size_bytes: Option<u64>,
        iceberg_delete_files: Option<Vec<String>>,
        metadata: Option<TableMetadata>,
        partition_spec: Option<PartitionSpec>,
        statistics: Option<TableStatistics>,
        parquet_metadata: Option<Arc<FileMetaData>>,
    },
    Database {
        path: String,
        size_bytes: Option<u64>,
        metadata: Option<TableMetadata>,
        statistics: Option<TableStatistics>,
    },
    #[cfg(feature = "python")]
    PythonFactoryFunction {
        module: String,
        func_name: String,
        func_args: python::PythonTablesFactoryArgs,
        size_bytes: Option<u64>,
        metadata: Option<TableMetadata>,
        statistics: Option<TableStatistics>,
        partition_spec: Option<PartitionSpec>,
    },
}

impl Hash for DataSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash everything except for cached parquet metadata.
        match self {
            Self::File {
                path,
                chunk_spec,
                size_bytes,
                iceberg_delete_files,
                metadata,
                partition_spec,
                statistics,
                ..
            } => {
                path.hash(state);
                if let Some(chunk_spec) = chunk_spec {
                    chunk_spec.hash(state);
                }
                size_bytes.hash(state);
                iceberg_delete_files.hash(state);
                metadata.hash(state);
                partition_spec.hash(state);
                statistics.hash(state);
            }
            Self::Database {
                path,
                size_bytes,
                metadata,
                statistics,
            } => {
                path.hash(state);
                size_bytes.hash(state);
                metadata.hash(state);
                statistics.hash(state);
            }
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction {
                module,
                func_name,
                func_args,
                size_bytes,
                metadata,
                statistics,
                partition_spec,
            } => {
                module.hash(state);
                func_name.hash(state);
                func_args.hash(state);
                size_bytes.hash(state);
                metadata.hash(state);
                statistics.hash(state);
                partition_spec.hash(state);
            }
        }
    }
}

impl DataSource {
    #[must_use]
    pub fn get_path(&self) -> &str {
        match self {
            Self::File { path, .. } | Self::Database { path, .. } => path,
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { module, .. } => module,
        }
    }

    #[must_use]
    pub fn get_parquet_metadata(&self) -> Option<&Arc<FileMetaData>> {
        match self {
            Self::File {
                parquet_metadata, ..
            } => parquet_metadata.as_ref(),
            _ => None,
        }
    }

    #[must_use]
    pub fn get_chunk_spec(&self) -> Option<&ChunkSpec> {
        match self {
            Self::File { chunk_spec, .. } => chunk_spec.as_ref(),
            Self::Database { .. } => None,
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { .. } => None,
        }
    }

    #[must_use]
    pub fn get_size_bytes(&self) -> Option<u64> {
        match self {
            Self::File { size_bytes, .. } | Self::Database { size_bytes, .. } => *size_bytes,
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { size_bytes, .. } => *size_bytes,
        }
    }

    #[must_use]
    pub fn get_metadata(&self) -> Option<&TableMetadata> {
        match self {
            Self::File { metadata, .. } | Self::Database { metadata, .. } => metadata.as_ref(),
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { metadata, .. } => metadata.as_ref(),
        }
    }

    #[must_use]
    pub fn get_statistics(&self) -> Option<&TableStatistics> {
        match self {
            Self::File { statistics, .. } | Self::Database { statistics, .. } => {
                statistics.as_ref()
            }
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { statistics, .. } => statistics.as_ref(),
        }
    }

    #[must_use]
    pub fn get_partition_spec(&self) -> Option<&PartitionSpec> {
        match self {
            Self::File { partition_spec, .. } => partition_spec.as_ref(),
            Self::Database { .. } => None,
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction { partition_spec, .. } => partition_spec.as_ref(),
        }
    }

    #[must_use]
    pub fn get_iceberg_delete_files(&self) -> Option<&Vec<String>> {
        match self {
            Self::File {
                iceberg_delete_files,
                ..
            } => iceberg_delete_files.as_ref(),
            _ => None,
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        match self {
            Self::File {
                path,
                chunk_spec,
                size_bytes,
                iceberg_delete_files,
                metadata,
                partition_spec,
                statistics,
                parquet_metadata: _,
            } => {
                res.push(format!("Path = {path}"));
                if let Some(chunk_spec) = chunk_spec {
                    res.push(format!(
                        "Chunk spec = {{ {} }}",
                        chunk_spec.multiline_display().join(", ")
                    ));
                }
                if let Some(size_bytes) = size_bytes {
                    res.push(format!("Size bytes = {size_bytes}"));
                }
                if let Some(iceberg_delete_files) = iceberg_delete_files {
                    res.push(format!("Iceberg delete files = {iceberg_delete_files:?}"));
                }
                if let Some(metadata) = metadata {
                    res.push(format!(
                        "Metadata = {}",
                        metadata.multiline_display().join(", ")
                    ));
                }
                if let Some(partition_spec) = partition_spec {
                    res.push(format!(
                        "Partition spec = {}",
                        partition_spec.multiline_display().join(", ")
                    ));
                }
                if let Some(statistics) = statistics {
                    res.push(format!("Statistics = {statistics}"));
                }
            }
            Self::Database {
                path,
                size_bytes,
                metadata,
                statistics,
            } => {
                res.push(format!("Path = {path}"));
                if let Some(size_bytes) = size_bytes {
                    res.push(format!("Size bytes = {size_bytes}"));
                }
                if let Some(metadata) = metadata {
                    res.push(format!(
                        "Metadata = {}",
                        metadata.multiline_display().join(", ")
                    ));
                }
                if let Some(statistics) = statistics {
                    res.push(format!("Statistics = {statistics}"));
                }
            }
            #[cfg(feature = "python")]
            Self::PythonFactoryFunction {
                module,
                func_name,
                func_args: _func_args,
                size_bytes,
                metadata,
                statistics,
                partition_spec,
            } => {
                res.push(format!("Function = {module}.{func_name}"));
                if let Some(size_bytes) = size_bytes {
                    res.push(format!("Size bytes = {size_bytes}"));
                }
                if let Some(metadata) = metadata {
                    res.push(format!(
                        "Metadata = {}",
                        metadata.multiline_display().join(", ")
                    ));
                }
                if let Some(partition_spec) = partition_spec {
                    res.push(format!(
                        "Partition spec = {}",
                        partition_spec.multiline_display().join(", ")
                    ));
                }
                if let Some(statistics) = statistics {
                    res.push(format!("Statistics = {statistics}"));
                }
            }
        }
        res
    }
}

impl DisplayAs for DataSource {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact | common_display::DisplayLevel::Default => {
                match self {
                    Self::File { path, .. } => {
                        format!("File {{{path}}}")
                    }
                    Self::Database { path, .. } => format!("Database {{{path}}}"),
                    #[cfg(feature = "python")]
                    Self::PythonFactoryFunction {
                        module, func_name, ..
                    } => {
                        format!("{module}:{func_name}")
                    }
                }
            }
            common_display::DisplayLevel::Verbose => self.multiline_display().join("\n"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Hash)]
pub struct ScanTask {
    pub sources: Vec<DataSource>,

    /// Schema to use when reading the DataSources.
    ///
    /// Schema to use when reading the DataSources. This should always be passed in by the
    /// ScanOperator implementation and should not have had any "pruning" applied.
    ///
    /// Note that this is different than the schema of the data after pushdowns have been applied,
    /// which can be obtained with [`ScanTask::materialized_schema`] instead.
    pub schema: SchemaRef,

    pub file_format_config: Arc<FileFormatConfig>,
    pub storage_config: Arc<StorageConfig>,
    pub pushdowns: Pushdowns,
    pub size_bytes_on_disk: Option<u64>,
    pub metadata: Option<TableMetadata>,
    pub statistics: Option<TableStatistics>,
    pub generated_fields: Option<SchemaRef>,
}

#[typetag::serde]
impl ScanTaskLike for ScanTask {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn dyn_eq(&self, other: &dyn ScanTaskLike) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|a| a == self)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }

    fn materialized_schema(&self) -> SchemaRef {
        self.materialized_schema()
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows()
    }

    fn approx_num_rows(&self, config: Option<&DaftExecutionConfig>) -> Option<f64> {
        self.approx_num_rows(config)
    }

    fn upper_bound_rows(&self) -> Option<usize> {
        self.upper_bound_rows()
    }

    fn size_bytes_on_disk(&self) -> Option<usize> {
        self.size_bytes_on_disk()
    }

    fn estimate_in_memory_size_bytes(&self, config: Option<&DaftExecutionConfig>) -> Option<usize> {
        self.estimate_in_memory_size_bytes(config)
    }

    fn file_format_config(&self) -> Arc<FileFormatConfig> {
        self.file_format_config.clone()
    }

    fn pushdowns(&self) -> &Pushdowns {
        &self.pushdowns
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn get_file_paths(&self) -> Vec<String> {
        self.sources
            .iter()
            .filter_map(|s| match s {
                DataSource::File { path, .. } => Some(path.clone()),
                _ => None,
            })
            .collect()
    }
}

impl From<ScanTask> for ScanTaskLikeRef {
    fn from(task: ScanTask) -> Self {
        Arc::new(task)
    }
}

pub type ScanTaskRef = Arc<ScanTask>;

static WARC_COLUMN_SIZES: OnceLock<HashMap<&'static str, usize>> = OnceLock::new();

fn warc_column_sizes() -> &'static HashMap<&'static str, usize> {
    WARC_COLUMN_SIZES.get_or_init(|| {
        let mut m = HashMap::new();
        // Average sizes based on analysis of Common Crawl WARC files.
        m.insert("WARC-Record-ID", 36); // UUID-style identifiers.
        m.insert("WARC-Type", 8); // e.g. "response".
        m.insert("WARC-Date", 8); // Timestamp stored as i64 nanoseconds.
        m.insert("Content-Length", 8); // i64.
        m.insert("WARC-Identified-Payload-Type", 5); // e.g. "text/html". Typically null.
        m.insert("warc_content", 27282); // Average content size.
        m.insert("warc_headers", 350); // Average headers size.
        m
    })
}

impl ScanTask {
    #[must_use]
    pub fn new(
        sources: Vec<DataSource>,
        file_format_config: Arc<FileFormatConfig>,
        schema: SchemaRef,
        storage_config: Arc<StorageConfig>,
        pushdowns: Pushdowns,
        generated_fields: Option<SchemaRef>,
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
                            curr_stats
                                .map(
                                    #[allow(deprecated)]
                                    |curr_stats| {
                                        let acc_stats = acc_stats.cast_to_schema(&schema)?;
                                        let curr_stats = curr_stats.cast_to_schema(&schema)?;
                                        acc_stats.union(&curr_stats)
                                    },
                                )
                                .transpose()
                                .ok()
                                .flatten()
                        }),
                    )
                },
            )
            .unwrap();
        let metadata = length.map(|l| TableMetadata { length: l });
        Self {
            sources,
            schema,
            file_format_config,
            storage_config,
            pushdowns,
            size_bytes_on_disk,
            metadata,
            statistics,
            generated_fields,
        }
    }

    pub fn merge(sc1: &Self, sc2: &Self) -> Result<Self, Error> {
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
        if sc1.generated_fields != sc2.generated_fields {
            return Err(Error::DifferingGeneratedFieldsInScanTaskMerge {
                fpc1: sc1.generated_fields.clone(),
                fpc2: sc2.generated_fields.clone(),
            });
        }
        Ok(Self::new(
            sc1.sources
                .clone()
                .into_iter()
                .chain(sc2.sources.clone())
                .collect(),
            sc1.file_format_config.clone(),
            sc1.schema.clone(),
            sc1.storage_config.clone(),
            sc1.pushdowns.clone(),
            sc1.generated_fields.clone(),
        ))
    }

    #[must_use]
    pub fn materialized_schema(&self) -> SchemaRef {
        match (&self.generated_fields, &self.pushdowns.columns) {
            (None, None) => self.schema.clone(),
            _ => {
                let schema_with_generated_fields =
                    if let Some(generated_fields) = &self.generated_fields {
                        // Extend the schema with generated fields.
                        Arc::new(self.schema.non_distinct_union(generated_fields).unwrap())
                    } else {
                        self.schema.clone()
                    };

                let mut fields = schema_with_generated_fields.fields().to_vec();

                // Filter the schema based on the pushdown column filters.
                if let Some(columns) = &self.pushdowns.columns {
                    fields.retain(|field| columns.contains(&field.name));
                }

                Arc::new(Schema::new(fields))
            }
        }
    }

    /// Obtain an accurate, exact num_rows from the ScanTask, or `None` if this is not possible
    #[must_use]
    pub fn num_rows(&self) -> Option<usize> {
        if self.pushdowns.filters.is_some() {
            // Cannot obtain an accurate num_rows if there are filters
            None
        } else {
            // Only can obtain an accurate num_rows if metadata is provided
            self.metadata.as_ref().map(|m| {
                // Account for any limit pushdowns
                if let Some(limit) = self.pushdowns.limit {
                    m.length.min(limit)
                } else {
                    m.length
                }
            })
        }
    }

    /// Obtain an approximate num_rows from the ScanTask, or `None` if this is not possible
    #[must_use]
    pub fn approx_num_rows(&self, config: Option<&DaftExecutionConfig>) -> Option<f64> {
        let approx_total_num_rows_before_pushdowns = self
            .metadata
            .as_ref()
            .map(|metadata| {
                // Use accurate metadata if available
                metadata.length as f64
            })
            .or_else(|| {
                // Otherwise, we fall back on estimations based on the file size
                // use inflation factor from config and estimate number of rows from the schema
                self.size_bytes_on_disk.map(|file_size| {
                    let config = config
                        .map_or_else(|| Cow::Owned(DaftExecutionConfig::default()), Cow::Borrowed);
                    let inflation_factor = match self.file_format_config.as_ref() {
                        FileFormatConfig::Parquet(_) => config.parquet_inflation_factor,
                        FileFormatConfig::Csv(_) | FileFormatConfig::Json(_) => {
                            config.csv_inflation_factor
                        }
                        FileFormatConfig::Warc(_) => {
                            if self.is_gzipped() {
                                5.0
                            } else {
                                1.0
                            }
                        }
                        #[cfg(feature = "python")]
                        FileFormatConfig::Database(_) => 1.0,
                        #[cfg(feature = "python")]
                        FileFormatConfig::PythonFunction => 1.0,
                    };
                    let in_mem_size: f64 = (file_size as f64) * inflation_factor;
                    let read_row_size = if self.is_warc() {
                        // Across 100 Common Crawl WARC files, the average record size is 470 (metadata) + 27282 (content) bytes.
                        // This is 27752 bytes per record.
                        27752.0
                    } else {
                        self.schema.estimate_row_size_bytes()
                    };
                    in_mem_size / read_row_size
                })
            });

        approx_total_num_rows_before_pushdowns.map(|approx_total_num_rows_before_pushdowns| {
            if self.pushdowns.filters.is_some() {
                // HACK: This might not be a good idea? We could also just return None here
                // Assume that filters filter out about 80% of the data
                let estimated_selectivity =
                    self.pushdowns.estimated_selectivity(self.schema.as_ref());
                // Set the lower bound approximated number of rows to 1 to avoid underestimation.
                (approx_total_num_rows_before_pushdowns * estimated_selectivity).max(1.0)
            } else if let Some(limit) = self.pushdowns.limit {
                (limit as f64).min(approx_total_num_rows_before_pushdowns)
            } else {
                approx_total_num_rows_before_pushdowns
            }
        })
    }

    /// Obtain the absolute maximum number of rows this ScanTask can give, or None if not possible to derive
    #[must_use]
    pub fn upper_bound_rows(&self) -> Option<usize> {
        self.metadata.as_ref().map(|m| {
            if let Some(limit) = self.pushdowns.limit {
                limit.min(m.length)
            } else {
                m.length
            }
        })
    }

    #[must_use]
    pub fn size_bytes_on_disk(&self) -> Option<usize> {
        self.size_bytes_on_disk.map(|s| s as usize)
    }

    fn is_warc(&self) -> bool {
        matches!(self.file_format_config.as_ref(), FileFormatConfig::Warc(_))
    }

    fn is_gzipped(&self) -> bool {
        self.sources
            .first()
            .and_then(|s| match s {
                DataSource::File { path, .. } => {
                    let filename = std::path::Path::new(path);
                    Some(
                        filename
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("gz")),
                    )
                }
                _ => None,
            })
            .unwrap_or(false)
    }

    #[must_use]
    pub fn estimate_in_memory_size_bytes(
        &self,
        config: Option<&DaftExecutionConfig>,
    ) -> Option<usize> {
        // WARC files that are gzipped are often 5x smaller than the uncompressed size.
        // For example, see this blog post by Common Crawl: https://commoncrawl.org/blog/february-2025-crawl-archive-now-available
        if self.is_warc() {
            let approx_num_rows = self.approx_num_rows(config)?;
            let mat_schema = self.materialized_schema();

            // Calculate size based on materialized schema and WARC column sizes
            let row_size: usize = mat_schema
                .field_names()
                .map(|name| warc_column_sizes().get(name).copied().unwrap_or(8))
                .sum();

            let estimate = (approx_num_rows * row_size as f64) as usize;
            Some(estimate)
        } else {
            let mat_schema = self.materialized_schema();
            self.statistics
                .as_ref()
                .and_then(|s| {
                    // Derive in-memory size estimate from table stats.
                    self.num_rows().and_then(|num_rows| {
                        #[allow(deprecated)]
                        let mat_stats = s.cast_to_schema(&mat_schema).ok()?;
                        let row_size = mat_stats.estimate_row_size().ok()?;
                        let estimate = (num_rows as f64) * row_size;
                        Some(estimate as usize)
                    })
                })
                .or_else(|| {
                    // use approximate number of rows multiplied by an approximate bytes-per-row
                    self.approx_num_rows(config).map(|approx_num_rows| {
                        let row_size = mat_schema.estimate_row_size_bytes();
                        let estimate = approx_num_rows * row_size;
                        estimate as usize
                    })
                })
        }
    }

    #[must_use]
    pub fn partition_spec(&self) -> Option<&PartitionSpec> {
        match self.sources.first() {
            None => None,
            Some(source) => source.get_partition_spec(),
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        // TODO(Clark): Use above methods to display some of the more derived fields.
        res.push(format!(
            "Sources = [ {} ]",
            self.sources
                .iter()
                .map(|s| s.multiline_display().join(", "))
                .join("; ")
        ));
        res.push(format!("Schema = {}", self.schema.short_string()));
        let file_format = self.file_format_config.multiline_display();
        if !file_format.is_empty() {
            res.push(format!(
                "{} config= {}",
                self.file_format_config.var_name(),
                file_format.join(", ")
            ));
        }
        let storage_config = self.storage_config.multiline_display();
        if !storage_config.is_empty() {
            res.push(format!(
                "storage config = {{ {} }}",
                storage_config.join(", ")
            ));
        }
        res.extend(self.pushdowns.multiline_display());
        if let Some(size_bytes) = self.size_bytes_on_disk {
            res.push(format!("Size bytes on disk = {size_bytes}"));
        }
        if let Some(metadata) = &self.metadata {
            res.push(format!(
                "Metadata = {}",
                metadata.multiline_display().join(", ")
            ));
        }
        if let Some(statistics) = &self.statistics {
            res.push(format!("Statistics = {statistics}"));
        }
        res
    }
}

impl DisplayAs for ScanTask {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        // take first 3 and last 3 if more than 6 sources
        let condensed_sources = if self.sources.len() <= 6 {
            self.sources.iter().map(|s| s.display_as(level)).join(", ")
        } else {
            let len = self.sources.len();
            self.sources
                .iter()
                .enumerate()
                .filter_map(|(i, s)| match i {
                    0..3 => Some(s.display_as(level)),
                    3 => Some("...".to_string()),
                    _ if i >= len - 3 => Some(s.display_as(level)),
                    _ => None,
                })
                .join(", ")
        };

        match level {
            common_display::DisplayLevel::Compact => {
                format!("{{{condensed_sources}}}",).trim_start().to_string()
            }
            common_display::DisplayLevel::Default => {
                format!(
                    "ScanTask:
Sources = [{condensed_sources}]
Pushdowns = {pushdowns}
",
                    pushdowns = self
                        .pushdowns
                        .display_as(common_display::DisplayLevel::Default)
                )
            }
            common_display::DisplayLevel::Verbose => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_display::{DisplayAs, DisplayLevel};
    use common_error::DaftResult;
    use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
    use common_scan_info::{Pushdowns, ScanOperator};
    use daft_schema::{schema::Schema, time_unit::TimeUnit};
    use itertools::Itertools;

    use crate::{glob::GlobScanOperator, storage_config::StorageConfig, DataSource, ScanTask};

    fn make_scan_task(num_sources: usize) -> ScanTask {
        let sources = (0..num_sources)
            .map(|i| DataSource::File {
                path: format!("test{i}"),
                chunk_spec: None,
                size_bytes: None,
                iceberg_delete_files: None,
                metadata: None,
                partition_spec: None,
                statistics: None,
                parquet_metadata: None,
            })
            .collect_vec();

        let file_format_config = FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit: TimeUnit::Seconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
        });

        ScanTask::new(
            sources,
            Arc::new(file_format_config),
            Arc::new(Schema::empty()),
            Arc::new(StorageConfig::new_internal(false, None)),
            Pushdowns::default(),
            None,
        )
    }

    async fn make_glob_scan_operator(num_sources: usize, infer_schema: bool) -> GlobScanOperator {
        let file_format_config: FileFormatConfig = FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit: TimeUnit::Seconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
        });

        let mut sources: Vec<String> = Vec::new();

        for _ in 0..num_sources {
            sources.push("../../tests/assets/parquet-data/mvp.parquet".to_string());
        }

        let glob_scan_operator: GlobScanOperator = GlobScanOperator::try_new(
            sources,
            Arc::new(file_format_config),
            Arc::new(StorageConfig::new_internal(false, None)),
            infer_schema,
            Some(Arc::new(Schema::empty())),
            None,
            false,
        )
        .await
        .unwrap();

        glob_scan_operator
    }

    #[tokio::test]
    async fn test_glob_display_condenses() -> DaftResult<()> {
        let glob_scan_operator: GlobScanOperator = make_glob_scan_operator(8, false).await;
        let condensed_glob_paths: Vec<String> = glob_scan_operator.multiline_display();
        assert_eq!(condensed_glob_paths[1], "Glob paths = [../../tests/assets/parquet-data/mvp.parquet, ../../tests/assets/parquet-data/mvp.parquet, ../../tests/assets/parquet-data/mvp.parquet, ..., ../../tests/assets/parquet-data/mvp.parquet, ../../tests/assets/parquet-data/mvp.parquet, ../../tests/assets/parquet-data/mvp.parquet]");
        Ok(())
    }

    #[tokio::test]
    async fn test_glob_single_file_stats() -> DaftResult<()> {
        let glob_scan_operator = make_glob_scan_operator(1, true).await;
        let scan_tasks = tokio::task::spawn_blocking(move || {
            glob_scan_operator.to_scan_tasks(Pushdowns::default())
        })
        .await
        .unwrap()?;
        assert_eq!(scan_tasks.len(), 1, "Expected 1 scan task");
        assert_eq!(
            scan_tasks[0].num_rows(),
            Some(100),
            "Expected 100 rows in the first scan task"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_glob_multiple_files_stats() -> DaftResult<()> {
        let glob_scan_operator = make_glob_scan_operator(8, true).await;
        let scan_tasks = tokio::task::spawn_blocking(move || {
            glob_scan_operator.to_scan_tasks(Pushdowns::default())
        })
        .await
        .unwrap()?;
        assert!(scan_tasks.len() > 1, "Expected more than 1 scan tasks");
        assert_eq!(scan_tasks[0].num_rows(), Some(100), "We should be able to populate stats from inference when the scan task's file matches the file used during inference");
        assert_eq!(scan_tasks[1].num_rows(), Some(100), "We should be able to populate stats from inference when the scan task's file matches the file used during inference");
        Ok(())
    }

    #[test]
    fn test_display_condenses() -> DaftResult<()> {
        let scan_task = make_scan_task(7);
        let condensed = scan_task.display_as(DisplayLevel::Compact);
        assert_eq!(condensed, "{File {test0}, File {test1}, File {test2}, ..., File {test4}, File {test5}, File {test6}}".to_string());
        Ok(())
    }

    #[test]
    fn test_display_no_condense() -> DaftResult<()> {
        let scan_task = make_scan_task(6);
        let condensed = scan_task.display_as(DisplayLevel::Compact);
        assert_eq!(
            condensed,
            "{File {test0}, File {test1}, File {test2}, File {test3}, File {test4}, File {test5}}"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_display_condenses_default() -> DaftResult<()> {
        let scan_task = make_scan_task(7);
        let condensed = scan_task.display_as(DisplayLevel::Default);
        assert_eq!(condensed, "ScanTask:\nSources = [File {test0}, File {test1}, File {test2}, ..., File {test4}, File {test5}, File {test6}]\nPushdowns = \n".to_string());
        Ok(())
    }

    #[test]
    fn test_display_no_condense_default() -> DaftResult<()> {
        let scan_task = make_scan_task(6);
        let condensed = scan_task.display_as(DisplayLevel::Default);
        assert_eq!(condensed, "ScanTask:\nSources = [File {test0}, File {test1}, File {test2}, File {test3}, File {test4}, File {test5}]\nPushdowns = \n".to_string());
        Ok(())
    }
}
