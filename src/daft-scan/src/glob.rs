use std::{fmt::Display, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::schema::SchemaRef;
use daft_csv::CsvParseOptions;
use daft_io::{
    get_io_client, get_runtime, parse_url, FileMetadata, IOClient, IOStatsContext, IOStatsRef,
};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use futures::{stream::BoxStream, StreamExt};
use snafu::{ResultExt, Snafu};
#[cfg(feature = "python")]
use {crate::PyIOSnafu, daft_core::schema::Schema, pyo3::Python};

use crate::{
    file_format::{CsvSourceConfig, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    DataFileSource, PartitionField, Pushdowns, ScanOperator, ScanTask, ScanTaskRef,
};
#[derive(Debug, PartialEq, Hash)]
pub struct GlobScanOperator {
    glob_paths: Vec<String>,
    file_format_config: Arc<FileFormatConfig>,
    schema: SchemaRef,
    storage_config: Arc<StorageConfig>,
}

/// Wrapper struct that implements a sync Iterator for a BoxStream
struct BoxStreamIterator<'a, T> {
    boxstream: BoxStream<'a, T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<'a, T> Iterator for BoxStreamIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime_handle
            .block_on(async { self.boxstream.next().await })
    }
}

#[derive(Snafu, Debug)]
enum Error {
    #[snafu(display("Glob path had no matches: \"{}\"", glob_path))]
    GlobNoMatch { glob_path: String },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        match &value {
            Error::GlobNoMatch { glob_path } => DaftError::FileNotFound {
                path: glob_path.clone(),
                source: Box::new(value),
            },
        }
    }
}

type FileInfoIterator = Box<dyn Iterator<Item = DaftResult<FileMetadata>>>;

fn run_glob(
    glob_path: &str,
    limit: Option<usize>,
    io_client: Arc<IOClient>,
    runtime: Arc<tokio::runtime::Runtime>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<FileInfoIterator> {
    let (_, parsed_glob_path) = parse_url(glob_path)?;

    // Construct a static-lifetime BoxStream returning the FileMetadata
    let glob_input = parsed_glob_path.as_ref().to_string();
    let runtime_handle = runtime.handle();
    let boxstream = runtime_handle.block_on(async move {
        io_client
            .glob(glob_input, None, None, limit, io_stats)
            .await
    })?;

    // Construct a static-lifetime BoxStreamIterator
    let iterator = BoxStreamIterator {
        boxstream,
        runtime_handle: runtime_handle.clone(),
    };
    let iterator = iterator.map(|fm| Ok(fm?));
    Ok(Box::new(iterator))
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
        glob_paths: &[&str],
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        schema: Option<SchemaRef>,
    ) -> DaftResult<Self> {
        let first_glob_path = match glob_paths.first() {
            None => Err(DaftError::ValueError(
                "Cannot glob empty list of files".to_string(),
            )),
            Some(path) => Ok(path),
        }?;

        let schema = match schema {
            Some(s) => s,
            None => {
                let (io_runtime, io_client) = get_io_client_and_runtime(storage_config.as_ref())?;
                let io_stats = IOStatsContext::new(format!(
                    "GlobScanOperator::try_new schema inference for {first_glob_path}"
                ));
                let mut paths = run_glob(
                    first_glob_path,
                    Some(1),
                    io_client.clone(),
                    io_runtime.clone(),
                    Some(io_stats.clone()),
                )?;
                let FileMetadata {
                    filepath: first_filepath,
                    ..
                } = match paths.next() {
                    Some(file_metadata) => file_metadata,
                    None => Err(Error::GlobNoMatch {
                        glob_path: first_glob_path.to_string(),
                    }
                    .into()),
                }?;
                let inferred_schema = match file_format_config.as_ref() {
                    FileFormatConfig::Parquet(ParquetSourceConfig {
                        coerce_int96_timestamp_unit,
                    }) => {
                        let io_stats = IOStatsContext::new(format!(
                            "GlobScanOperator constructor read_parquet_schema: for uri {first_filepath}"
                        ));
                        daft_parquet::read::read_parquet_schema(
                            first_filepath.as_str(),
                            io_client.clone(),
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
                        quote,
                        escape_char,
                        comment,
                        ..
                    }) => {
                        let (schema, _) = daft_csv::metadata::read_csv_schema(
                            first_filepath.as_str(),
                            Some(CsvParseOptions::new_with_defaults(
                                *has_headers,
                                *delimiter,
                                *double_quote,
                                *quote,
                                *escape_char,
                                *comment,
                            )?),
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
                                    first_filepath.as_str(),
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
            glob_paths: glob_paths.iter().map(|s| s.to_string()).collect(),
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
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'static>> {
        let (io_runtime, io_client) = get_io_client_and_runtime(self.storage_config.as_ref())?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::to_scan_tasks for {:#?}",
            self.glob_paths
        ));

        // Run [`run_glob`] on each path and mux them into the same iterator
        let files = self
            .glob_paths
            .clone()
            .into_iter()
            .flat_map(move |glob_path| {
                match run_glob(
                    glob_path.as_str(),
                    None,
                    io_client.clone(),
                    io_runtime.clone(),
                    Some(io_stats.clone()),
                ) {
                    Ok(paths) => paths,
                    Err(err) => Box::new(vec![Err(err)].into_iter()),
                }
            });

        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        // Create one ScanTask per file
        Ok(Box::new(files.map(move |f| {
            let FileMetadata {
                filepath: path,
                size: size_bytes,
                ..
            } = f?;
            Ok(ScanTask::new(
                vec![DataFileSource::AnonymousDataFile {
                    path: path.to_string(),
                    chunk_spec: None,
                    size_bytes,
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                }],
                file_format_config.clone(),
                schema.clone(),
                storage_config.clone(),
                pushdowns.clone(),
            )
            .into())
        })))
    }
}
