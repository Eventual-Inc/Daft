use std::{
    collections::VecDeque,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime, get_io_runtime};
use daft_core::prelude::*;
use daft_io::{IOConfig, SourceType, parse_url, utils::ObjectPath};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
#[allow(deprecated)]
use parquet::{
    arrow::{
        ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata,
        arrow_writer::{ArrowColumnChunk, ArrowLeafColumn, compute_leaves, get_column_writers},
    },
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::types::{ColumnPath, SchemaDescriptor},
};

use crate::{
    AsyncFileWriter, WriteResult,
    storage_backend::{FileStorageBackend, ObjectStorageBackend, StorageBackend},
    utils::{build_filename, build_filename_single},
};

type ColumnWriterFuture = dyn Future<Output = DaftResult<ArrowColumnChunk>> + Send;

/// Parse a user-supplied compression name into a `parquet::basic::Compression` codec.
///
/// Codec strings are case-insensitive. `level` is applied to codecs that take one (`zstd`,
/// `gzip`, `brotli`) and ignored by codecs that do not; `None` selects the codec's default level
/// (zstd 1, gzip 6, brotli 1). Out-of-range levels are rejected with a `ValueError`.
pub(crate) fn parse_compression(s: &str, level: Option<i32>) -> DaftResult<Compression> {
    let codec = s.to_ascii_lowercase();
    // parquet-rs reports the valid range in its error, so surface that directly. Negative levels
    // for the unsigned codecs are mapped to `u32::MAX` so they fail the same range check.
    let level_err = |err: parquet::errors::ParquetError| {
        DaftError::ValueError(format!(
            "invalid compression level {} for parquet codec {codec}: {err}",
            level.unwrap_or_default()
        ))
    };
    let unsigned_level = || level.map(|l| u32::try_from(l).unwrap_or(u32::MAX));
    match codec.as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(match unsigned_level() {
            Some(l) => GzipLevel::try_new(l).map_err(level_err)?,
            None => GzipLevel::default(),
        })),
        "lzo" => Ok(Compression::LZO),
        "brotli" => Ok(Compression::BROTLI(match unsigned_level() {
            Some(l) => BrotliLevel::try_new(l).map_err(level_err)?,
            None => BrotliLevel::default(),
        })),
        "lz4" => Ok(Compression::LZ4),
        "lz4_raw" => Ok(Compression::LZ4_RAW),
        "zstd" => Ok(Compression::ZSTD(match level {
            Some(l) => ZstdLevel::try_new(l).map_err(level_err)?,
            None => ZstdLevel::default(),
        })),
        other => Err(DaftError::ValueError(format!(
            "unsupported parquet compression codec: {other}"
        ))),
    }
}

/// Whether a parsed codec carries a compression level.
fn compression_has_level(compression: Compression) -> bool {
    matches!(
        compression,
        Compression::ZSTD(_) | Compression::GZIP(_) | Compression::BROTLI(_)
    )
}

/// Resolve the default codec and the per-column overrides for a Parquet write.
///
/// `compression_level` is applied to every codec in use that supports a level (`zstd`, `gzip`,
/// `brotli`), so it reaches both the default codec and any `column_compression` override. It is
/// an error to supply a level when none of the requested codecs can honor it, so a
/// misconfiguration such as `compression="snappy", compression_level=6` is never silently ignored.
///
/// Returns `(default_compression, [(dot-separated column path, compression)])`.
pub(crate) fn resolve_parquet_compression(
    compression: Option<&str>,
    column_compression: Option<&[(String, String)]>,
    compression_level: Option<i32>,
) -> DaftResult<(Compression, Vec<(String, Compression)>)> {
    let default_compression = match compression {
        Some(name) => parse_compression(name, compression_level)?,
        None => Compression::SNAPPY,
    };
    let parsed_column_compression: Vec<(String, Compression)> = column_compression
        .unwrap_or(&[])
        .iter()
        .map(|(path, name)| Ok((path.clone(), parse_compression(name, compression_level)?)))
        .collect::<DaftResult<_>>()?;

    if let Some(level) = compression_level
        && !compression_has_level(default_compression)
        && !parsed_column_compression
            .iter()
            .any(|(_, c)| compression_has_level(*c))
    {
        let mut codecs: Vec<String> = std::iter::once(compression.unwrap_or("snappy"))
            .chain(
                column_compression
                    .unwrap_or(&[])
                    .iter()
                    .map(|(_, name)| name.as_str()),
            )
            .map(str::to_ascii_lowercase)
            .collect();
        codecs.sort();
        codecs.dedup();
        return Err(DaftError::ValueError(format!(
            "compression_level={level} requires a codec that supports compression levels (zstd, gzip, brotli), but only {} requested",
            codecs.join(", ")
        )));
    }

    Ok((default_compression, parsed_column_compression))
}

/// Construct writer properties for the native Parquet writer.
///
/// `default_compression` applies to every column unless overridden by `column_compression`.
/// Each entry in `column_compression` is `(dot-separated column path, parsed compression)`.
fn native_parquet_writer_properties(
    arrow_schema: &arrow_schema::Schema,
    default_compression: Compression,
    column_compression: &[(String, Compression)],
) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(default_compression);
    for (path, compression) in column_compression {
        let parts: Vec<String> = path.split('.').map(str::to_string).collect();
        builder = builder.set_column_compression(ColumnPath::new(parts), *compression);
    }
    let mut props = builder.build();
    add_encoded_arrow_schema_to_metadata(arrow_schema, &mut props);
    props
}

/// Helper function that checks if we support native writes given the file format, path, and schema.
pub(crate) fn native_parquet_writer_supported(
    root_dir: &str,
    file_schema: &SchemaRef,
) -> DaftResult<bool> {
    let (source_type, _) = parse_url(root_dir)?;
    if !source_type.supports_native_writer() {
        return Ok(false);
    }

    let Ok(arrow_schema) = file_schema.to_arrow() else {
        return Ok(false);
    };

    // Schema convertibility is independent of the chosen compression, so use defaults here.
    let writer_properties =
        native_parquet_writer_properties(&arrow_schema, Compression::SNAPPY, &[]);
    Ok(ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .is_ok())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_native_parquet_writer(
    root_dir: &str,
    schema: &SchemaRef,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
    compression: Option<&str>,
    column_compression: Option<&[(String, String)]>,
    compression_level: Option<i32>,
    single_file: bool,
    overwrite_single_file_target: bool,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Option<RecordBatch>>>> {
    // Parse the root directory and add partition values if present.
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = if single_file {
        build_filename_single(&source_type, root_dir.as_ref())?
    } else {
        build_filename(
            &source_type,
            root_dir.as_ref(),
            partition_values,
            file_idx,
            "parquet",
        )?
    };

    let (default_compression, parsed_column_compression) =
        resolve_parquet_compression(compression, column_compression, compression_level)?;

    // TODO(desmond): Explore configurations such data page size limit, writer version, etc. Parquet format v2
    // could be interesting but has much less support in the ecosystem (including ourselves).
    let arrow_schema = Arc::new(schema.to_arrow()?.into());
    let writer_properties = native_parquet_writer_properties(
        &arrow_schema,
        default_compression,
        &parsed_column_compression,
    );

    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .expect("By this point `native_writer_supported` should have been called which would have verified that the schema is convertible");

    match source_type {
        SourceType::File => {
            let storage_backend = FileStorageBackend {};
            Ok(Box::new(ParquetWriter::new(
                filename,
                Arc::new(writer_properties),
                arrow_schema,
                parquet_schema,
                partition_values.cloned(),
                storage_backend,
                overwrite_single_file_target,
            )))
        }
        source if source.supports_native_writer() => {
            let ObjectPath { scheme, .. } = daft_io::utils::parse_object_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError("IO config is required for object writes".to_string())
            })?;
            let storage_backend = ObjectStorageBackend::new(scheme, io_config);
            Ok(Box::new(ParquetWriter::new(
                filename,
                Arc::new(writer_properties),
                arrow_schema,
                parquet_schema,
                partition_values.cloned(),
                storage_backend,
                false,
            )))
        }
        _ => Err(DaftError::InternalError(format!(
            "Unsupported source type for the native writer: {source_type}"
        ))),
    }
}

fn remove_existing_local_target(filename: &Path) -> DaftResult<()> {
    if filename.is_dir() {
        std::fs::remove_dir_all(filename)?;
    } else if filename.exists() {
        std::fs::remove_file(filename)?;
    }
    Ok(())
}

struct ParquetWriter<B: StorageBackend> {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
    file_writer: Option<SerializedFileWriter<B::Writer>>,
    total_bytes_written: usize,
    overwrite_existing_target: bool,
}

impl<B: StorageBackend> ParquetWriter<B> {
    const PATH_FIELD_NAME: &str = "path";

    fn new(
        filename: PathBuf,
        writer_properties: Arc<WriterProperties>,
        arrow_schema: Arc<arrow_schema::Schema>,
        parquet_schema: SchemaDescriptor,
        partition_values: Option<RecordBatch>,
        storage_backend: B,
        overwrite_existing_target: bool,
    ) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values,
            storage_backend,
            file_writer: None,
            total_bytes_written: 0,
            overwrite_existing_target,
        }
    }

    async fn create_writer(&mut self) -> DaftResult<()> {
        if self.overwrite_existing_target {
            remove_existing_local_target(&self.filename)?;
        }
        let backend_writer = self.storage_backend.create_writer(&self.filename).await?;
        let file_writer = SerializedFileWriter::new(
            backend_writer,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Some(file_writer);
        Ok(())
    }

    fn extract_leaf_columns_from_record_batches(
        &self,
        record_batches: &[RecordBatch],
        num_leaf_columns: usize,
    ) -> DaftResult<Vec<Vec<ArrowLeafColumn>>> {
        // Preallocate a vector for each leaf column across all record batches.
        let mut leaf_columns: Vec<Vec<ArrowLeafColumn>> = (0..num_leaf_columns)
            .map(|_| Vec::with_capacity(record_batches.len()))
            .collect();
        // Iterate through each record batch and extract its leaf columns.
        for record_batch in record_batches {
            let arrays = record_batch.get_inner_arrow_arrays();
            let mut leaf_column_slots = leaf_columns.iter_mut();

            for (arr, field) in arrays.zip(&self.arrow_schema.fields) {
                let leaves = compute_leaves(field, &arr)
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                for leaf in leaves {
                    match leaf_column_slots.next() {
                        Some(slot) => slot.push(leaf),
                        None => {
                            return Err(DaftError::InternalError(
                                "Mismatch between leaves and column slots".to_string(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(leaf_columns)
    }

    /// Helper function to create (but not spawn) futures, where each future encodes one arrow leaf
    /// column. The futures are returned in the same order in which they're supposed to appear in
    /// the parquet file.
    fn build_column_writer_futures(
        &self,
        record_batches: &[RecordBatch],
    ) -> DaftResult<VecDeque<Pin<Box<ColumnWriterFuture>>>> {
        // Get leaf column writers. For example, a struct<int, int> column produces two leaf column writers.
        #[allow(deprecated)]
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

        // Flatten record batches into per-leaf-column Arrow data chunks.
        let leaf_columns =
            self.extract_leaf_columns_from_record_batches(record_batches, column_writers.len())?;
        let compute_futures: VecDeque<_> = column_writers
            .into_iter()
            .zip(leaf_columns.into_iter())
            .map(|(mut column_writer, leaf_columns)| {
                let boxed = Box::pin(async move {
                    for chunk in leaf_columns {
                        column_writer
                            .write(&chunk)
                            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                    }

                    let chunk = column_writer
                        .close()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                    Ok(chunk)
                });

                boxed as Pin<Box<dyn Future<Output = DaftResult<ArrowColumnChunk>> + Send>>
            })
            .collect();

        Ok(compute_futures)
    }
}

#[async_trait]
impl<B: StorageBackend> AsyncFileWriter for ParquetWriter<B> {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        if self.file_writer.is_none() {
            self.create_writer().await?;
        }
        let num_rows = data.len();
        let record_batches = data.record_batches();

        let row_group_writer_thread_handle = {
            // Wait for the workers to complete encoding, and append the resulting column chunks to the row group and the file.
            let (tx_chunk, mut rx_chunk) = tokio::sync::mpsc::channel::<ArrowColumnChunk>(1);

            let mut file_writer = self.file_writer.take().unwrap();
            let io_runtime = get_io_runtime(true);

            // Spawn a thread to handle the row group writing since it involves blocking writes.
            let row_group_writer_thread_handle =
                io_runtime.spawn_blocking(move || -> DaftResult<SerializedFileWriter<_>> {
                    let mut row_group_writer = file_writer
                        .next_row_group()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                    while let Some(chunk) = rx_chunk.blocking_recv() {
                        chunk
                            .append_to_row_group(&mut row_group_writer)
                            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                    }

                    row_group_writer
                        .close()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                    Ok(file_writer)
                });

            let mut pending_column_writers = self.build_column_writer_futures(record_batches)?;

            // Spawn up to NUM_CPU workers to handle the column writes.
            let initial_spawn_count =
                get_compute_pool_num_threads().min(pending_column_writers.len());
            let mut spawned_column_writers: VecDeque<_> =
                VecDeque::with_capacity(initial_spawn_count);

            let compute_runtime = get_compute_runtime();

            for _ in 0..initial_spawn_count {
                if let Some(future) = pending_column_writers.pop_front() {
                    spawned_column_writers.push_back(compute_runtime.spawn(future));
                } else {
                    break; // No more futures to spawn
                }
            }

            while let Some(first_spawned_writer) = spawned_column_writers.pop_front() {
                let chunk = first_spawned_writer.await??;
                tx_chunk
                    .send(chunk)
                    .await
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                // Spawn a new task for the next column writer, if more columns are available.
                if let Some(next_pending_future) = pending_column_writers.pop_front() {
                    spawned_column_writers.push_back(compute_runtime.spawn(next_pending_future));
                }
            }

            row_group_writer_thread_handle
            // tx_chunk is dropped here, which signals the row writer thread to finish.
        };

        let file_writer = row_group_writer_thread_handle
            .await
            .map_err(|e| DaftError::ParquetError(e.to_string()))??;

        let bytes_written = file_writer.bytes_written() - self.total_bytes_written;
        self.total_bytes_written = file_writer.bytes_written();
        self.file_writer.replace(file_writer);

        Ok(WriteResult {
            bytes_written,
            rows_written: num_rows,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.

        // Our file writer might be backed by an S3 part writer that may block when flushing metadata.
        let io_runtime = get_io_runtime(true);
        let mut file_writer = self.file_writer.take().unwrap();
        self.file_writer = Some(
            io_runtime
                .spawn_blocking(move || -> DaftResult<SerializedFileWriter<_>> {
                    file_writer
                        .finish()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                    Ok(file_writer)
                })
                .await
                .map_err(|e| DaftError::ParquetError(e.to_string()))??,
        );

        // TODO: We can start encoding the next file while this finalization happens.

        // Let the storage backend handle its finalization. For our S3 backend, this waits for all
        // part uploads to complete.
        self.storage_backend.finalize().await?;

        // Return a recordbatch containing the filename that we wrote to.
        let field = Field::new(Self::PATH_FIELD_NAME, DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Arc::new(arrow_array::LargeStringArray::from_iter_values(
                std::iter::once(&self.filename.to_string_lossy()),
            )),
        )?;
        let record_batch =
            RecordBatch::new_with_size(Schema::new(vec![field]), vec![filename_series], 1)?;
        let record_batch_with_partition_values =
            if let Some(partition_values) = self.partition_values.take() {
                record_batch.union(&partition_values)?
            } else {
                record_batch
            };
        Ok(Some(record_batch_with_partition_values))
    }

    fn bytes_written(&self) -> usize {
        self.total_bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.total_bytes_written]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use bytes::Bytes;
    use parquet::{
        arrow::{ARROW_SCHEMA_META_KEY, ArrowWriter, parquet_to_arrow_schema},
        file::reader::{FileReader, SerializedFileReader},
    };

    use super::*;

    #[test]
    fn native_parquet_writer_properties_embeds_arrow_schema_metadata() {
        let daft_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Uuid)]));
        assert!(native_parquet_writer_supported("file:///tmp", &daft_schema).unwrap());

        let arrow_schema = daft_schema.to_arrow().expect("Conversion should pass");
        assert!(
            arrow_schema
                .field(0)
                .extension_type_name()
                .is_some_and(|n| n == "arrow.uuid")
        );

        let props = native_parquet_writer_properties(&arrow_schema, Compression::SNAPPY, &[]);
        let kv = props
            .key_value_metadata()
            .expect("expected key_value_metadata");
        assert!(kv.iter().any(|entry| entry.key == ARROW_SCHEMA_META_KEY));
    }

    #[test]
    fn parse_compression_handles_known_codecs() {
        for (input, expected) in [
            ("snappy", Compression::SNAPPY),
            ("SNAPPY", Compression::SNAPPY),
            ("none", Compression::UNCOMPRESSED),
            ("uncompressed", Compression::UNCOMPRESSED),
            ("lz4", Compression::LZ4),
            ("lz4_raw", Compression::LZ4_RAW),
        ] {
            assert_eq!(parse_compression(input, None).unwrap(), expected);
        }
        assert!(matches!(
            parse_compression("zstd", None).unwrap(),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            parse_compression("gzip", None).unwrap(),
            Compression::GZIP(_)
        ));
        assert!(parse_compression("bogus", None).is_err());
    }

    #[test]
    fn parse_compression_default_levels_match_parquet_rs_defaults() {
        assert_eq!(
            parse_compression("zstd", None).unwrap(),
            Compression::ZSTD(ZstdLevel::default())
        );
        assert_eq!(ZstdLevel::default().compression_level(), 1);
        assert_eq!(
            parse_compression("gzip", None).unwrap(),
            Compression::GZIP(GzipLevel::default())
        );
        assert_eq!(
            parse_compression("brotli", None).unwrap(),
            Compression::BROTLI(BrotliLevel::default())
        );
    }

    #[test]
    fn parse_compression_applies_level_to_leveled_codecs() {
        assert_eq!(
            parse_compression("zstd", Some(9)).unwrap(),
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
        );
        assert_eq!(
            parse_compression("ZSTD", Some(22)).unwrap(),
            Compression::ZSTD(ZstdLevel::try_new(22).unwrap())
        );
        assert_eq!(
            parse_compression("gzip", Some(9)).unwrap(),
            Compression::GZIP(GzipLevel::try_new(9).unwrap())
        );
        assert_eq!(
            parse_compression("brotli", Some(11)).unwrap(),
            Compression::BROTLI(BrotliLevel::try_new(11).unwrap())
        );
        // Codecs without a level ignore it; `resolve_parquet_compression` decides whether
        // that is acceptable for the write as a whole.
        assert_eq!(
            parse_compression("snappy", Some(9)).unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            parse_compression("none", Some(9)).unwrap(),
            Compression::UNCOMPRESSED
        );
    }

    #[test]
    fn parse_compression_rejects_out_of_range_levels() {
        for (codec, level) in [
            ("zstd", 0),
            ("zstd", 23),
            ("zstd", -1),
            ("gzip", 10),
            ("gzip", -1),
            ("brotli", 12),
            ("brotli", -3),
        ] {
            let err = parse_compression(codec, Some(level))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!(
                    "invalid compression level {level} for parquet codec {codec}"
                )),
                "unexpected error for {codec}/{level}: {err}"
            );
            assert!(
                err.contains("valid compression range"),
                "error should carry the valid range: {err}"
            );
        }
    }

    #[test]
    fn resolve_parquet_compression_applies_level_to_default_and_overrides() {
        let overrides = vec![
            ("a".to_string(), "snappy".to_string()),
            ("b".to_string(), "gzip".to_string()),
        ];
        let (default, columns) =
            resolve_parquet_compression(Some("zstd"), Some(&overrides), Some(6)).unwrap();
        assert_eq!(default, Compression::ZSTD(ZstdLevel::try_new(6).unwrap()));
        assert_eq!(
            columns,
            vec![
                ("a".to_string(), Compression::SNAPPY),
                (
                    "b".to_string(),
                    Compression::GZIP(GzipLevel::try_new(6).unwrap())
                ),
            ]
        );
    }

    #[test]
    fn resolve_parquet_compression_level_reaches_override_when_default_has_no_level() {
        let overrides = vec![("text".to_string(), "zstd".to_string())];
        let (default, columns) =
            resolve_parquet_compression(Some("snappy"), Some(&overrides), Some(9)).unwrap();
        assert_eq!(default, Compression::SNAPPY);
        assert_eq!(
            columns,
            vec![(
                "text".to_string(),
                Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
            )]
        );
    }

    #[test]
    fn resolve_parquet_compression_rejects_level_without_leveled_codec() {
        let err = resolve_parquet_compression(Some("snappy"), None, Some(6))
            .unwrap_err()
            .to_string();
        assert!(err.contains("compression_level=6"), "{err}");
        assert!(err.contains("only snappy requested"), "{err}");

        let overrides = vec![("a".to_string(), "lz4_raw".to_string())];
        let err = resolve_parquet_compression(None, Some(&overrides), Some(6))
            .unwrap_err()
            .to_string();
        assert!(err.contains("only lz4_raw, snappy requested"), "{err}");

        // No level: nothing to complain about.
        let (default, _) = resolve_parquet_compression(Some("snappy"), None, None).unwrap();
        assert_eq!(default, Compression::SNAPPY);
    }

    #[test]
    fn resolve_parquet_compression_surfaces_unknown_codec_before_level_check() {
        let err = resolve_parquet_compression(Some("bogus"), None, Some(6))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unsupported parquet compression codec: bogus"),
            "{err}"
        );
    }

    #[test]
    fn native_parquet_writer_properties_applies_per_column_compression() {
        let daft_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]));
        let arrow_schema = daft_schema.to_arrow().unwrap();

        let overrides = vec![("a".to_string(), Compression::SNAPPY)];
        let props = native_parquet_writer_properties(
            &arrow_schema,
            Compression::ZSTD(Default::default()),
            &overrides,
        );

        assert_eq!(
            props.compression(&ColumnPath::from("a")),
            Compression::SNAPPY,
        );
        assert!(matches!(
            props.compression(&ColumnPath::from("b")),
            Compression::ZSTD(_),
        ));
    }

    #[test]
    fn native_parquet_writer_properties_carries_compression_level() {
        let daft_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]));
        let arrow_schema = daft_schema.to_arrow().unwrap();

        let overrides = vec![("a".to_string(), "snappy".to_string())];
        let (default, columns) =
            resolve_parquet_compression(Some("zstd"), Some(&overrides), Some(19)).unwrap();
        let props = native_parquet_writer_properties(&arrow_schema, default, &columns);

        assert_eq!(
            props.compression(&ColumnPath::from("a")),
            Compression::SNAPPY,
        );
        assert_eq!(
            props.compression(&ColumnPath::from("b")),
            Compression::ZSTD(ZstdLevel::try_new(19).unwrap()),
        );
    }

    #[test]
    fn parquet_file_round_trips_extension_metadata() {
        let daft_schema = Schema::new(vec![Field::new("id", DataType::Uuid)]);
        let arrow_schema = Arc::new(daft_schema.to_arrow().unwrap());
        let expected_ext = arrow_schema
            .field(0)
            .extension_type_name()
            .map(str::to_string);

        let props = native_parquet_writer_properties(&arrow_schema, Compression::SNAPPY, &[]);
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props))
                .expect("ArrowWriter::try_new");
            let batch = RecordBatch::new_empty(arrow_schema);
            writer.write(&batch).expect("write empty batch");
            writer.close().expect("close writer");
        }

        let reader = SerializedFileReader::new(Bytes::from(buffer)).expect("read parquet");
        let file_meta = reader.metadata().file_metadata();
        let inferred =
            parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
                .expect("parquet_to_arrow_schema");

        assert_eq!(
            inferred.field(0).extension_type_name().map(str::to_string),
            expected_ext
        );
    }
}
