use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_io::{get_io_client, get_runtime, IOStatsContext};
use daft_parquet::read::ParquetSchemaInferenceOptions;

use crate::{
    file_format::{CsvSourceConfig, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    DataFileSource, PartitionField, Pushdowns, ScanOperator, ScanTask,
};
#[derive(Debug, PartialEq, Hash)]
pub struct GlobScanOperator {
    glob_path: String,
    file_format_config: Arc<FileFormatConfig>,
    schema: SchemaRef,
    storage_config: Arc<StorageConfig>,
}

fn run_glob(
    glob_path: &str,
    io_config: Arc<daft_io::IOConfig>,
    limit: Option<usize>,
) -> DaftResult<Vec<String>> {
    // Use multi-threaded runtime which should be global Arc-ed cached singletons
    let runtime = get_runtime(true)?;
    let io_client = get_io_client(true, io_config)?;

    let _rt_guard = runtime.enter();
    runtime.block_on(async {
        Ok(io_client
            .as_ref()
            .glob(glob_path, None, None, limit, None)
            .await?
            .into_iter()
            .map(|fm| fm.filepath)
            .collect())
    })
}

impl GlobScanOperator {
    pub fn _try_new(
        glob_path: &str,
        file_format_config: FileFormatConfig,
        storage_config: Arc<StorageConfig>,
    ) -> DaftResult<Self> {
        let io_config = match storage_config.as_ref() {
            StorageConfig::Native(cfg) => Arc::new(cfg.io_config.clone().unwrap_or_default()),
            #[cfg(feature = "python")]
            StorageConfig::Python(cfg) => Arc::new(cfg.io_config.clone().unwrap_or_default()),
        };
        let paths = run_glob(glob_path, io_config.clone(), Some(1))?;
        let first_filepath = paths[0].as_str();

        let schema = match &file_format_config {
            FileFormatConfig::Parquet(ParquetSourceConfig {
                coerce_int96_timestamp_unit,
                row_groups: _,
            }) => {
                let io_client = get_io_client(true, io_config.clone())?; // it appears that read_parquet_schema is hardcoded to use multithreaded_io
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator constructor read_parquet_schema: for uri {first_filepath}"
                ));
                daft_parquet::read::read_parquet_schema(
                    first_filepath,
                    io_client,
                    Some(io_stats),
                    ParquetSchemaInferenceOptions {
                        coerce_int96_timestamp_unit: *coerce_int96_timestamp_unit,
                    },
                )?
            }
            FileFormatConfig::Csv(CsvSourceConfig {
                delimiter,
                has_headers,
                double_quote,
                buffer_size: _,
                chunk_size: _,
            }) => {
                let io_client = get_io_client(true, io_config.clone())?; // it appears that read_parquet_schema is hardcoded to use multithreaded_io
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator constructor read_csv_schema: for uri {first_filepath}"
                ));
                let (schema, _, _, _, _) = daft_csv::metadata::read_csv_schema(
                    first_filepath,
                    *has_headers,
                    Some(delimiter.as_bytes()[0]),
                    *double_quote,
                    None,
                    io_client,
                    Some(io_stats),
                )?;
                schema
            }
            FileFormatConfig::Json(JsonSourceConfig {}) => {
                // NOTE: Native JSON reads not yet implemented, so we have to delegate to Python here or implement
                // a daft_json crate that gives us native JSON schema inference
                todo!("Implement schema inference from JSON in GlobScanOperator");
            }
        };

        Ok(Self {
            glob_path: glob_path.to_string(),
            file_format_config: Arc::new(file_format_config),
            schema: Arc::new(schema),
            storage_config,
        })
    }
}

impl Display for GlobScanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl ScanOperator for GlobScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }
    fn can_absorb_select(&self) -> bool {
        false
    }
    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<crate::ScanTask>>>> {
        let io_config = match self.storage_config.as_ref() {
            StorageConfig::Native(cfg) => Arc::new(cfg.io_config.clone().unwrap_or_default()),
            #[cfg(feature = "python")]
            StorageConfig::Python(cfg) => Arc::new(cfg.io_config.clone().unwrap_or_default()),
        };
        let columns = pushdowns.columns;
        let limit = pushdowns.limit;

        // Clone to move into closure for delayed execution
        let storage_config = self.storage_config.clone();
        let schema = self.schema.clone();
        let file_format_config = self.file_format_config.clone();

        let files = run_glob(self.glob_path.as_str(), io_config, None)?;
        let iter = files.into_iter().map(move |f| {
            let source = DataFileSource::AnonymousDataFile {
                path: f,
                metadata: None,
                partition_spec: None,
                statistics: None,
            };
            Ok(ScanTask {
                source,
                file_format_config: file_format_config.clone(),
                schema: schema.clone(),
                storage_config: storage_config.clone(),
                columns: columns.clone(),
                limit,
            })
        });
        Ok(Box::new(iter))
    }
}
