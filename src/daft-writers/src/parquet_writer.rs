use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_runtime::{get_compute_runtime, RuntimeTask};
use daft_core::prelude::*;
use daft_io::{parse_url, SourceType};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use parquet::{
    arrow::{
        arrow_writer::{compute_leaves, get_column_writers, ArrowColumnChunk, ArrowLeafColumn},
        ArrowSchemaConverter,
    },
    basic::Compression,
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::types::SchemaDescriptor,
};

use crate::{utils::record_batch_to_partition_path, AsyncFileWriter};

type ParquetColumnWriterHandle = RuntimeTask<DaftResult<ArrowColumnChunk>>;

/// Helper function that checks if we support native writes given the file format, root directory, and schema.
pub(crate) fn native_parquet_writer_supported(
    file_format: FileFormat,
    root_dir: &str,
    file_schema: &SchemaRef,
) -> DaftResult<bool> {
    // TODO(desmond): Currently we only support native parquet writes.
    if !matches!(file_format, FileFormat::Parquet) {
        return Ok(false);
    }
    // TODO(desmond): Currently we only support local writes.
    // TODO(desmond): We return false on an error because S3n is incorrectly reported to be unsupported
    // in parse_url. I'll fix this in another PR.
    let (source_type, _) = parse_url(root_dir)?;
    if !matches!(source_type, SourceType::File) {
        return Ok(false);
    }
    // TODO(desmond): Currently we do not support extension and timestamp types.
    let arrow_schema = match file_schema.to_arrow() {
        Ok(schema)
            if schema
                .fields
                .iter()
                .all(|field| field.data_type().can_convert_to_arrow_rs()) =>
        {
            Arc::new(schema.into())
        }
        _ => return Ok(false),
    };
    let writer_properties = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    Ok(ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .is_ok())
}

pub(crate) fn create_native_parquet_writer(
    root_dir: &str,
    schema: &SchemaRef,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    // Parse the root directory and add partition values if present.
    let (source_type, root_dir) = parse_url(root_dir)?;
    debug_assert!(
        matches!(source_type, SourceType::File),
        "Native writes are currently enabled for local writes only"
    );
    let root_dir = Path::new(root_dir.trim_start_matches("file://"));
    let dir = if let Some(partition_values) = partition_values {
        let partition_path = record_batch_to_partition_path(partition_values, None)?;
        root_dir.join(partition_path)
    } else {
        root_dir.to_path_buf()
    };
    // Create the directories if they don't exist.
    std::fs::create_dir_all(&dir)?;

    let filename = dir.join(format!("{}-{}.parquet", uuid::Uuid::new_v4(), file_idx));

    // TODO(desmond): Explore configurations such data page size limit, writer version, etc. Parquet format v2
    // could be interesting but has much less support in the ecosystem (including ourselves).
    let writer_properties = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build(),
    );

    let arrow_schema = Arc::new(schema.to_arrow()?.into());

    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(writer_properties.coerce_types())
        .convert(&arrow_schema)
        .expect("By this point `native_writer_supported` should have been called which would have verified that the schema is convertible");

    Ok(Box::new(ParquetWriter::new(
        filename,
        writer_properties,
        arrow_schema,
        parquet_schema,
        partition_values.cloned(),
    )))
}

struct ParquetWriter {
    filename: PathBuf,
    writer_properties: Arc<WriterProperties>,
    arrow_schema: Arc<arrow_schema::Schema>,
    parquet_schema: SchemaDescriptor,
    partition_values: Option<RecordBatch>,
    file_writer: Option<SerializedFileWriter<BufWriter<std::fs::File>>>,
}

impl ParquetWriter {
    /// TODO(desmond): This can be tuned.
    /// Default buffer size for writing to files.
    const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
    const PATH_FIELD_NAME: &str = "path";

    fn new(
        filename: PathBuf,
        writer_properties: Arc<WriterProperties>,
        arrow_schema: Arc<arrow_schema::Schema>,
        parquet_schema: SchemaDescriptor,
        partition_values: Option<RecordBatch>,
    ) -> Self {
        Self {
            filename,
            writer_properties,
            arrow_schema,
            parquet_schema,
            partition_values,
            file_writer: None,
        }
    }

    fn create_writer(&mut self) -> DaftResult<()> {
        let file = std::fs::File::create(&self.filename)?;
        let bufwriter = BufWriter::with_capacity(Self::DEFAULT_WRITE_BUFFER_SIZE, file);
        let writer = SerializedFileWriter::new(
            bufwriter,
            self.parquet_schema.root_schema_ptr(),
            self.writer_properties.clone(),
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        self.file_writer = Some(writer);
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
                let leaves = compute_leaves(field, &arr.into())
                    .map_err(|e| DaftError::ParquetError(e.to_string()))?;

                for leaf in leaves {
                    match leaf_column_slots.next() {
                        Some(slot) => slot.push(leaf),
                        None => {
                            return Err(DaftError::InternalError(
                                "Mismatch between leaves and column slots".to_string(),
                            ))
                        }
                    }
                }
            }
        }
        Ok(leaf_columns)
    }

    /// Helper function that spawns 1 worker thread per leaf column, dispatches the relevant arrow leaf columns to each
    /// worker, then returns the handles to the workers.
    fn spawn_column_writer_workers(
        &self,
        record_batches: &[RecordBatch],
    ) -> DaftResult<Vec<ParquetColumnWriterHandle>> {
        // Get leaf column writers. For example, a struct<int, int> column produces two leaf column writers.
        let column_writers = get_column_writers(
            &self.parquet_schema,
            &self.writer_properties,
            &self.arrow_schema,
        )
        .map_err(|e| DaftError::ParquetError(e.to_string()))?;

        // Flatten record batches into per-leaf-column Arrow data chunks.
        let leaf_columns =
            self.extract_leaf_columns_from_record_batches(record_batches, column_writers.len())?;

        let compute_runtime = get_compute_runtime();

        // Spawn one worker per leaf column writer.
        Ok(column_writers
            .into_iter()
            .zip(leaf_columns.into_iter())
            .map(|(mut writer, leaf_column)| {
                compute_runtime.spawn(async move {
                    for chunk in leaf_column {
                        writer
                            .write(&chunk)
                            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
                    }
                    writer
                        .close()
                        .map_err(|e| DaftError::ParquetError(e.to_string()))
                })
            })
            .collect())
    }
}

#[async_trait]
impl AsyncFileWriter for ParquetWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        if self.file_writer.is_none() {
            self.create_writer()?;
        }
        let starting_bytes_written = self.bytes_written();
        let record_batches = data.get_tables()?;

        // Spawn column writers.
        let column_writer_handles = self.spawn_column_writer_workers(&record_batches)?;

        // Wait for the workers to complete encoding, and append the resulting column chunks to the row group and the file.
        let mut row_group_writer = self
            .file_writer
            .as_mut()
            .expect("File writer should be created by now")
            .next_row_group()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        for handle in column_writer_handles {
            let chunk = handle.await??;
            chunk
                .append_to_row_group(&mut row_group_writer)
                .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        }

        // Close the current row group.
        row_group_writer
            .close()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;

        Ok(self.bytes_written() - starting_bytes_written)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        // TODO(desmond): We can shove some pretty useful metadata before closing the file.
        let _metadata = self
            .file_writer
            .as_mut()
            .expect("File writer should be created by now")
            .finish()
            .map_err(|e| DaftError::ParquetError(e.to_string()))?;
        // Return a recordbatch containing the filename that we wrote to.
        let field = Field::new(Self::PATH_FIELD_NAME, DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Box::new(arrow2::array::Utf8Array::<i64>::from_slice([&self
                .filename
                .to_string_lossy()])),
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
        self.file_writer
            .as_ref()
            .expect("File writer should be created by now")
            .bytes_written()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        let bytes_written = self
            .file_writer
            .as_ref()
            .expect("File writer should be created by now")
            .bytes_written();
        vec![bytes_written]
    }
}
