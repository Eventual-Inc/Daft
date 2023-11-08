use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_io::{get_io_client, get_runtime, parse_url, IOClient, IOStatsContext, IOStatsRef};
use daft_parquet::read::ParquetSchemaInferenceOptions;
#[cfg(feature = "python")]
use {crate::PyIOSnafu, daft_core::schema::Schema, pyo3::Python, snafu::ResultExt};

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
    limit: Option<usize>,
    io_client: Arc<IOClient>,
    runtime: Arc<tokio::runtime::Runtime>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Vec<String>> {
    let (_, parsed_glob_path) = parse_url(glob_path)?;
    let _rt_guard = runtime.enter();
    runtime.block_on(async {
        Ok(io_client
            .as_ref()
            .glob(&parsed_glob_path, None, None, limit, io_stats)
            .await?
            .into_iter()
            .map(|fm| fm.filepath)
            .collect())
    })
}

fn get_io_client_and_runtime(
    storage_config: &StorageConfig,
) -> DaftResult<(Arc<tokio::runtime::Runtime>, Arc<IOClient>)> {
    // Grab an IOClient and Runtime
    // TODO: This should be cleaned up and hidden behind a better API from daft-io
    match storage_config {
        StorageConfig::Native(cfg) => {
            let multithreaded_io = cfg.multithreaded_io;
            Ok((
                get_runtime(multithreaded_io)?,
                get_io_client(
                    multithreaded_io,
                    Arc::new(cfg.io_config.clone().unwrap_or_default()),
                )?,
            ))
        }
        #[cfg(feature = "python")]
        StorageConfig::Python(cfg) => {
            let multithreaded_io = true; // Hardcode to use multithreaded IO if Python storage config is used for data fetches
            Ok((
                get_runtime(multithreaded_io)?,
                get_io_client(
                    multithreaded_io,
                    Arc::new(cfg.io_config.clone().unwrap_or_default()),
                )?,
            ))
        }
    }
}

impl GlobScanOperator {
    pub fn try_new(
        glob_path: &str,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        schema: Option<SchemaRef>,
    ) -> DaftResult<Self> {
        let schema = match schema {
            Some(s) => s,
            None => {
                let (io_runtime, io_client) = get_io_client_and_runtime(storage_config.as_ref())?;
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator::try_new schema inference for {glob_path}"
                ));
                let paths = run_glob(
                    glob_path,
                    Some(1),
                    io_client.clone(),
                    io_runtime,
                    Some(io_stats.clone()),
                )?;
                let first_filepath = paths[0].as_str();
                let inferred_schema = match file_format_config.as_ref() {
                    FileFormatConfig::Parquet(ParquetSourceConfig {
                        coerce_int96_timestamp_unit,
                        ..
                    }) => daft_parquet::read::read_parquet_schema(
                        first_filepath,
                        io_client.clone(),
                        Some(io_stats),
                        ParquetSchemaInferenceOptions {
                            coerce_int96_timestamp_unit: *coerce_int96_timestamp_unit,
                        },
                    )?,
                    FileFormatConfig::Csv(CsvSourceConfig {
                        delimiter,
                        has_headers,
                        double_quote,
                        ..
                    }) => {
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
                        match storage_config.as_ref() {
                            StorageConfig::Native(_) => todo!(
                                "Implement native JSON schema inference in a daft_json crate."
                            ),
                            #[cfg(feature = "python")]
                            StorageConfig::Python(_) => Python::with_gil(|py| {
                                crate::python::pylib::read_json_schema(
                                    py,
                                    first_filepath,
                                    storage_config.clone().into(),
                                )
                                .and_then(|s| {
                                    Ok(Schema::new(s.schema.fields.values().cloned().collect())?)
                                })
                                .context(PyIOSnafu)
                            })?,
                        }
                    }
                };
                Arc::new(inferred_schema)
            }
        };

        Ok(Self {
            glob_path: glob_path.to_string(),
            file_format_config,
            schema,
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
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTask>>>> {
        let (io_runtime, io_client) = get_io_client_and_runtime(self.storage_config.as_ref())?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::to_scan_tasks for {}",
            self.glob_path
        ));

        // TODO: This runs the glob to exhaustion, but we should return an iterator instead
        let files = run_glob(
            self.glob_path.as_str(),
            None,
            io_client,
            io_runtime,
            Some(io_stats),
        )?;
        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        // Create one ScanTask per file. We should find a way to perform streaming from the glob instead
        // of materializing here.
        Ok(Box::new(files.into_iter().map(move |f| {
            Ok(ScanTask::new(
                vec![DataFileSource::AnonymousDataFile {
                    path: f.to_string(),
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                }],
                file_format_config.clone(),
                schema.clone(),
                storage_config.clone(),
                pushdowns.columns.clone(),
                pushdowns.limit,
            ))
        })))
    }
}
