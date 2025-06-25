use std::{sync::Arc, vec};

use common_error::{DaftError, DaftResult};
use common_file_formats::{CsvSourceConfig, FileFormat, FileFormatConfig, ParquetSourceConfig};
use common_runtime::RuntimeRef;
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};
use daft_core::{prelude::Utf8Array, series::IntoSeries};
use daft_csv::CsvParseOptions;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::{parse_url, FileMetadata, IOClient, IOStatsContext, IOStatsRef};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_recordbatch::RecordBatch;
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
};
use daft_stats::{PartitionSpec, TableMetadata};
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use snafu::Snafu;

use crate::{
    hive::{hive_partitions_to_fields, hive_partitions_to_series, parse_hive_partitioning},
    storage_config::StorageConfig,
    ChunkSpec, DataSource, ScanTask,
};
#[derive(Debug)]
pub struct GlobScanOperator {
    glob_paths: Vec<String>,
    file_format_config: Arc<FileFormatConfig>,
    schema: SchemaRef,
    storage_config: Arc<StorageConfig>,
    file_path_column: Option<String>,
    hive_partitioning: bool,
    partitioning_keys: Vec<PartitionField>,
    generated_fields: SchemaRef,
    // When creating the glob scan operator, we might collect file metadata for the first file during schema inference.
    // Cache this metadata (along with the first filepath) so we can use it to populate the stats for the first scan task.
    first_metadata: Option<(String, TableMetadata)>,
}

/// Wrapper struct that implements a sync Iterator for a BoxStream
struct BoxStreamIterator<'a, T> {
    boxstream: BoxStream<'a, T>,
    runtime_handle: RuntimeRef,
}

impl<T> Iterator for BoxStreamIterator<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime_handle
            .block_on_current_thread(async { self.boxstream.next().await })
    }
}

#[derive(Snafu, Debug)]
enum Error {
    #[snafu(display(
        "Glob path had no matches: \"{}\". \nTo search for files recursively, use '{}/**'.",
        glob_path,
        glob_path.trim_end_matches('/'),
    ))]
    GlobNoMatch { glob_path: String },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        match &value {
            Error::GlobNoMatch { glob_path } => Self::FileNotFound {
                path: glob_path.clone(),
                source: Box::new(value),
            },
        }
    }
}

async fn run_glob(
    glob_path: &str,
    limit: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    file_format: FileFormat,
) -> DaftResult<impl Stream<Item = DaftResult<FileMetadata>> + Send> {
    let (_, parsed_glob_path) = parse_url(glob_path)?;
    // Construct a static-lifetime BoxStream returning the FileMetadata
    let glob_input = parsed_glob_path.as_ref().to_string();
    let stream = io_client
        .glob(glob_input, None, None, limit, io_stats, Some(file_format))
        .await?;

    let stream = stream.map_err(|e| e.into());

    Ok(stream)
}

fn run_glob_parallel(
    glob_paths: Vec<String>,
    io_client: Arc<IOClient>,
    runtime: RuntimeRef,
    io_stats: Option<IOStatsRef>,
    file_format: FileFormat,
) -> DaftResult<impl Iterator<Item = DaftResult<FileMetadata>>> {
    let num_parallel_tasks = 64;

    let owned_runtime = runtime.clone();
    let boxstream = futures::stream::iter(glob_paths.into_iter().map(move |path| {
        let (_, parsed_glob_path) = parse_url(&path).unwrap();
        let glob_input = parsed_glob_path.as_ref().to_string();
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();

        runtime.spawn(async move {
            let stream = io_client
                .glob(glob_input, None, None, None, io_stats, Some(file_format))
                .await?;
            let results = stream.map_err(|e| e.into()).collect::<Vec<_>>().await;
            DaftResult::Ok(futures::stream::iter(results))
        })
    }))
    .buffered(num_parallel_tasks)
    .map(|stream| stream?)
    .try_flatten()
    .boxed();

    // Construct a static-lifetime BoxStreamIterator
    let iterator = BoxStreamIterator {
        boxstream,
        runtime_handle: owned_runtime,
    };
    Ok(iterator)
}

impl GlobScanOperator {
    pub async fn try_new(
        glob_paths: Vec<String>,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        infer_schema: bool,
        user_provided_schema: Option<SchemaRef>,
        file_path_column: Option<String>,
        hive_partitioning: bool,
    ) -> DaftResult<Self> {
        let first_glob_path = match glob_paths.first() {
            None => Err(DaftError::ValueError(
                "Cannot glob empty list of files".to_string(),
            )),
            Some(path) => Ok(path),
        }?;

        let file_format = file_format_config.file_format();

        let (_, io_client) = storage_config.get_io_client_and_runtime()?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::try_new schema inference for {first_glob_path}"
        ));
        let mut paths = run_glob(
            first_glob_path,
            Some(1),
            io_client.clone(),
            Some(io_stats.clone()),
            file_format,
        )
        .await?;

        let FileMetadata {
            filepath: first_filepath,
            ..
        } = match paths.next().await {
            Some(file_metadata) => file_metadata,
            None => Err(Error::GlobNoMatch {
                glob_path: first_glob_path.to_string(),
            }
            .into()),
        }?;
        // If hive partitioning is set, create partition fields from the hive partitions.
        let mut partition_fields = if hive_partitioning {
            let hive_partitions = parse_hive_partitioning(&first_filepath)?;
            hive_partitions_to_fields(&hive_partitions)
        } else {
            vec![]
        };
        // If file path column is set, extend the partition fields.
        if let Some(fp_col) = &file_path_column {
            let fp_field = Field::new(fp_col, DataType::Utf8);
            partition_fields.push(fp_field);
        }
        let (partitioning_keys, generated_fields) = if partition_fields.is_empty() {
            (vec![], Schema::empty())
        } else {
            let generated_fields = Schema::new(partition_fields);
            let generated_fields = match user_provided_schema.clone() {
                Some(hint) => generated_fields.apply_hints(&hint)?,
                None => generated_fields,
            };
            // Extract partitioning keys only after the user's schema hints have been applied.
            let partitioning_keys = generated_fields
                .into_iter()
                .map(|field| PartitionField::new(field.clone(), None, None))
                .collect::<Result<Vec<_>, _>>()?;
            (partitioning_keys, generated_fields)
        };

        let (schema, first_metadata) = match infer_schema {
            true => {
                let (inferred_schema, first_metadata) = match file_format_config.as_ref() {
                    &FileFormatConfig::Parquet(ParquetSourceConfig {
                        coerce_int96_timestamp_unit,
                        ref field_id_mapping,
                        ..
                    }) => {
                        let io_stats = IOStatsContext::new(format!(
                            "GlobScanOperator constructor read_parquet_schema: for uri {first_filepath}"
                        ));

                        let (schema, metadata) =
                            daft_parquet::read::read_parquet_schema_and_metadata(
                                first_filepath.as_str(),
                                io_client,
                                Some(io_stats),
                                ParquetSchemaInferenceOptions {
                                    coerce_int96_timestamp_unit,
                                    ..Default::default()
                                },
                                field_id_mapping.clone(),
                            )
                            .await?;
                        let metadata = Some((
                            first_filepath,
                            TableMetadata {
                                length: metadata.num_rows,
                            },
                        ));
                        (schema, metadata)
                    }
                    FileFormatConfig::Csv(CsvSourceConfig {
                        delimiter,
                        has_headers,
                        double_quote,
                        quote,
                        escape_char,
                        comment,
                        allow_variable_columns,
                        ..
                    }) => {
                        let (schema, _) = daft_csv::metadata::read_csv_schema(
                            first_filepath.as_str(),
                            Some(CsvParseOptions::new_with_defaults(
                                *has_headers,
                                *delimiter,
                                *double_quote,
                                *quote,
                                *allow_variable_columns,
                                *escape_char,
                                *comment,
                            )?),
                            None,
                            io_client,
                            Some(io_stats),
                        )
                        .await?;
                        (schema, None)
                    }
                    FileFormatConfig::Json(_) => {
                        let schema = daft_json::schema::read_json_schema(
                            first_filepath.as_str(),
                            None,
                            None,
                            io_client,
                            Some(io_stats),
                        )
                        .await?;
                        (schema, None)
                    }
                    FileFormatConfig::Warc(_) => {
                        return Err(DaftError::ValueError(
                            "Warc schemas do not need to be inferred".to_string(),
                        ))
                    }
                    #[cfg(feature = "python")]
                    FileFormatConfig::Database(_) => {
                        return Err(DaftError::ValueError(
                            "Cannot glob a database source".to_string(),
                        ))
                    }
                    #[cfg(feature = "python")]
                    FileFormatConfig::PythonFunction => {
                        return Err(DaftError::ValueError(
                            "Cannot glob a PythonFunction source".to_string(),
                        ))
                    }
                };
                match user_provided_schema {
                    Some(hint) => (
                        Arc::new(inferred_schema.apply_hints(&hint)?),
                        first_metadata,
                    ),
                    None => (Arc::new(inferred_schema), first_metadata),
                }
            }
            false => (
                user_provided_schema.expect("Schema must be provided if infer_schema is false"),
                None,
            ),
        };
        Ok(Self {
            glob_paths,
            file_format_config,
            schema,
            storage_config,
            file_path_column,
            hive_partitioning,
            partitioning_keys,
            generated_fields: Arc::new(generated_fields),
            first_metadata,
        })
    }
}

impl ScanOperator for GlobScanOperator {
    fn name(&self) -> &'static str {
        "GlobScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &self.partitioning_keys
    }

    fn file_path_column(&self) -> Option<&str> {
        self.file_path_column.as_deref()
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        Some(self.generated_fields.clone())
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

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        let condensed_glob_paths = if self.glob_paths.len() <= 7 {
            self.glob_paths.join(", ")
        } else {
            let first_three: Vec<String> = self.glob_paths.iter().take(3).cloned().collect();
            let last_three: Vec<String> = self
                .glob_paths
                .iter()
                .skip(self.glob_paths.len() - 3)
                .cloned()
                .collect();

            let mut result = first_three.join(", ");
            result.push_str(", ...");
            result.push_str(", ");
            result.push_str(&last_three.join(", "));

            result
        };

        let mut lines = vec![
            "GlobScanOperator".to_string(),
            format!("Glob paths = [{}]", condensed_glob_paths),
        ];
        lines.extend(self.file_format_config.multiline_display());
        lines.extend(self.storage_config.multiline_display());

        lines
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let (io_runtime, io_client) = self.storage_config.get_io_client_and_runtime()?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::to_scan_tasks for {:#?}",
            self.glob_paths
        ));
        let file_format = self.file_format_config.file_format();

        let files = run_glob_parallel(
            self.glob_paths.clone(),
            io_client,
            io_runtime,
            Some(io_stats),
            file_format,
        )?;

        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        let row_groups = if let FileFormatConfig::Parquet(ParquetSourceConfig {
            row_groups: Some(row_groups),
            ..
        }) = self.file_format_config.as_ref()
        {
            Some(row_groups.clone())
        } else {
            None
        };
        let file_path_column = self.file_path_column.clone();
        let hive_partitioning = self.hive_partitioning;
        let partition_fields = self
            .partitioning_keys
            .iter()
            .map(|partition_spec| partition_spec.clone_field());
        let partition_schema = Schema::new(partition_fields);
        let (first_filepath, first_metadata) =
            if let Some((first_filepath, first_metadata)) = &self.first_metadata {
                (Some(first_filepath), Some(first_metadata))
            } else {
                (None, None)
            };
        // Create one ScanTask per file.
        files
            .enumerate()
            .filter_map(|(idx, f)| {
                let scan_task_result = (|| {
                    let FileMetadata {
                        filepath: path,
                        size: size_bytes,
                        ..
                    } = f?;
                    // Create partition values from hive partitions, if any.
                    let mut partition_values = if hive_partitioning {
                        let hive_partitions = parse_hive_partitioning(&path)?;
                        hive_partitions_to_series(&hive_partitions, &partition_schema)?
                    } else {
                        vec![]
                    };
                    // Extend partition values based on whether a file_path_column is set (this column is inherently a partition).
                    if let Some(fp_col) = &file_path_column {
                        let trimmed = path.trim_start_matches("file://");
                        let file_paths_column_series =
                            Utf8Array::from_iter(fp_col, std::iter::once(Some(trimmed)))
                                .into_series();
                        partition_values.push(file_paths_column_series);
                    }
                    let (partition_spec, generated_fields) = if !partition_values.is_empty() {
                        let partition_values_table =
                            RecordBatch::from_nonempty_columns(partition_values)?;
                        // If there are partition values, evaluate them against partition filters, if any.
                        if let Some(partition_filters) = &pushdowns.partition_filters {
                            let partition_filters = BoundExpr::try_new(
                                partition_filters.clone(),
                                &partition_values_table.schema,
                            )?;
                            let filter_result =
                                partition_values_table.filter(&[partition_filters])?;
                            if filter_result.is_empty() {
                                // Skip the current file since it does not satisfy the partition filters.
                                return Ok(None);
                            }
                        }
                        let generated_fields = partition_values_table.schema.clone();
                        let partition_spec = PartitionSpec {
                            keys: partition_values_table,
                        };
                        (Some(partition_spec), Some(generated_fields))
                    } else {
                        (None, None)
                    };
                    let row_group = row_groups
                        .as_ref()
                        .and_then(|rgs| rgs.get(idx).cloned())
                        .flatten();
                    let chunk_spec = row_group.map(ChunkSpec::Parquet);
                    Ok(Some(ScanTask::new(
                        vec![DataSource::File {
                            metadata: if let Some(first_filepath) = first_filepath
                                && path == *first_filepath
                            {
                                first_metadata.cloned()
                            } else {
                                None
                            },
                            path,
                            chunk_spec,
                            size_bytes,
                            iceberg_delete_files: None,
                            partition_spec,
                            statistics: None,
                            parquet_metadata: None,
                        }],
                        file_format_config.clone(),
                        schema.clone(),
                        storage_config.clone(),
                        pushdowns.clone(),
                        generated_fields,
                    )))
                })();
                match scan_task_result {
                    Ok(Some(scan_task)) => Some(Ok(Arc::new(scan_task) as Arc<dyn ScanTaskLike>)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect()
    }
}
