use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_avro::{AvroCompression, AvroWriteOptions, write_record_batch_to_avro};
use daft_io::IOConfig;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, WriteResult};

pub struct NativeAvroWriter {
    root_dir: String,
    file_idx: usize,
    options: AvroWriteOptions,
    io_config: Option<IOConfig>,
    /// Accumulated record batches from all write() calls, flushed at close().
    batches: Vec<RecordBatch>,
    bytes_written: usize,
    rows_written: usize,
    closed: bool,
    /// Partition values to include in the result RecordBatch (Hive-style partitioned writes).
    partition_values: Option<RecordBatch>,
}

impl NativeAvroWriter {
    pub fn new(
        root_dir: &str,
        file_idx: usize,
        options: AvroWriteOptions,
        io_config: Option<IOConfig>,
        partition_values: Option<RecordBatch>,
    ) -> Self {
        Self {
            root_dir: root_dir.to_string(),
            file_idx,
            options,
            io_config,
            batches: Vec::new(),
            bytes_written: 0,
            rows_written: 0,
            closed: false,
            partition_values,
        }
    }

    fn file_path(&self) -> DaftResult<String> {
        let uuid_str = uuid::Uuid::new_v4();
        if let Some(ref partition_values) = self.partition_values {
            // Build Hive-style partition path: key1=value1/key2=value2/file.avro
            let partition_path = partition_values
                .columns()
                .iter()
                .map(|col| {
                    let key = urlencoding::encode(col.name());
                    if col.is_valid(0) {
                        let value = col.str_value(0).map_err(|e| {
                            common_error::DaftError::InternalError(format!(
                                "Failed to extract partition value: {}",
                                e
                            ))
                        })?;
                        Ok(format!("{}={}", key, urlencoding::encode(&value)))
                    } else {
                        Ok(format!("{}={}", key, "__HIVE_DEFAULT_PARTITION__"))
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?
                .join("/");
            // Strip trailing slash from root_dir if present for clean path joining
            let root = self.root_dir.trim_end_matches('/');
            Ok(format!(
                "{}/{}/{}-{}.avro",
                root, partition_path, uuid_str, self.file_idx
            ))
        } else {
            Ok(format!(
                "{}/{}-{}.avro",
                self.root_dir, uuid_str, self.file_idx
            ))
        }
    }
}

#[async_trait]
impl AsyncFileWriter for NativeAvroWriter {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, input: Self::Input) -> DaftResult<WriteResult> {
        let records = input.record_batches().to_vec();
        if records.is_empty() {
            return Ok(WriteResult {
                bytes_written: 0,
                rows_written: 0,
            });
        }

        let table = RecordBatch::concat(&records)?;
        let rows = table.num_rows();
        // Accumulate batches to be serialized once at close() to avoid
        // overwriting previous writes (Avro OCF has a single schema header).
        self.batches.push(table);
        self.rows_written += rows;

        Ok(WriteResult {
            bytes_written: 0,
            rows_written: rows,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if self.closed {
            return Ok(None);
        }
        self.closed = true;

        // Serialize all accumulated batches into a single Avro OCF.
        let table = RecordBatch::concat(&self.batches)?;
        let avro_bytes = write_record_batch_to_avro(&table, &self.options)?;
        self.bytes_written = avro_bytes.len();

        let file_path = self.file_path()?;
        // Ensure parent directory exists for local file writes.
        if let Some(parent) = std::path::Path::new(&file_path).parent()
            && let Some(local_path) = parent.to_str()
        {
            let local_path = daft_io::strip_file_uri_to_path(local_path).unwrap_or(local_path);
            std::fs::create_dir_all(local_path).map_err(|e| {
                common_error::DaftError::External(
                    format!("Unable to create directory {}: {}", local_path, e).into(),
                )
            })?;
        }
        let io_config = Arc::new(self.io_config.clone().unwrap_or_default());
        let io_client = daft_io::get_io_client(true, io_config)?;
        let data = bytes::Bytes::from(avro_bytes);
        io_client
            .single_url_put(&file_path, data, None)
            .await
            .map_err(|e| common_error::DaftError::External(e.into()))?;

        // Return a RecordBatch containing the file path
        use daft_core::prelude::*;
        let path_field = Field::new("path", DataType::Utf8);
        let path_series = Series::from_arrow(
            std::sync::Arc::new(path_field.clone()),
            std::sync::Arc::new(arrow_array::LargeStringArray::from_iter_values(
                std::iter::once(file_path.as_str()),
            )),
        )?;
        let record_batch =
            RecordBatch::new_with_size(Schema::new(vec![path_field]), vec![path_series], 1)?;
        // Include partition values in the result, matching ParquetWriter behavior.
        let record_batch_with_partition_values =
            if let Some(partition_values) = self.partition_values.take() {
                record_batch.union(&partition_values)?
            } else {
                record_batch
            };
        Ok(Some(record_batch_with_partition_values))
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }
}

pub fn create_native_avro_writer(
    root_dir: &str,
    file_idx: usize,
    compression: &str,
    io_config: Option<IOConfig>,
    partition_values: Option<&RecordBatch>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Option<RecordBatch>>>> {
    let codec: AvroCompression = compression
        .parse()
        .map_err(|e: daft_avro::AvroError| common_error::DaftError::External(e.into()))?;

    let options = AvroWriteOptions {
        compression: codec,
        record_name: "record".to_string(),
        record_namespace: String::new(),
    };

    Ok(Box::new(NativeAvroWriter::new(
        root_dir,
        file_idx,
        options,
        io_config,
        partition_values.cloned(),
    )))
}
