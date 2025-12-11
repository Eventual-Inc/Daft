use std::{path::PathBuf, sync::Arc};

use arrow_array::RecordBatch as ArrowRecordBatch;
use async_trait::async_trait;
use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, WriteResult, storage_backend::StorageBackend,
};

type WriteFn<W> = Arc<dyn Fn(&mut W, &[ArrowRecordBatch]) -> DaftResult<()> + Send + Sync>;
type CloseFn<W> = Arc<dyn Fn(W) -> DaftResult<()> + Send + Sync>;

pub struct BatchFileWriter<B: StorageBackend, W> {
    filename: PathBuf,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
    file_writer: Option<W>,
    bytes_written: usize,
    inflation_factor: f64,
    builder_fn: Arc<dyn Fn(B::Writer) -> W + Send + Sync>,
    write_fn: WriteFn<W>,
    close_fn: Option<CloseFn<W>>,
}

impl<B: StorageBackend, W> BatchFileWriter<B, W> {
    pub fn new(
        filename: PathBuf,
        partition_values: Option<RecordBatch>,
        storage_backend: B,
        inflation_factor: f64,
        builder_fn: Arc<dyn Fn(B::Writer) -> W + Send + Sync>,
        write_fn: WriteFn<W>,
        close_fn: Option<CloseFn<W>>,
    ) -> Self {
        Self {
            filename,
            partition_values,
            storage_backend,
            file_writer: None,
            bytes_written: 0,
            inflation_factor,
            builder_fn,
            write_fn,
            close_fn,
        }
    }

    fn estimate_bytes_to_write(&self, data: &MicroPartition) -> DaftResult<usize> {
        let base_size = data.size_bytes();
        Ok((base_size as f64 * self.inflation_factor) as usize)
    }

    async fn create_writer(&mut self) -> DaftResult<()> {
        let backend_writer = self.storage_backend.create_writer(&self.filename).await?;
        let file_writer = (self.builder_fn)(backend_writer);
        self.file_writer = Some(file_writer);
        Ok(())
    }
}

#[async_trait]
impl<B: StorageBackend + Send + Sync, W: Send + Sync + 'static> AsyncFileWriter
    for BatchFileWriter<B, W>
{
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        if self.file_writer.is_none() {
            self.create_writer().await?;
        }
        let num_rows = data.len();
        let est_bytes_to_write = self.estimate_bytes_to_write(&data)?;
        self.bytes_written += est_bytes_to_write;

        let record_batches = data.record_batches();
        let record_batches: Vec<ArrowRecordBatch> = record_batches
            .iter()
            .map(|rb| rb.clone().try_into())
            .collect::<DaftResult<_>>()?;

        let mut file_writer = self.file_writer.take().expect("writer created");
        let write_fn = self.write_fn.clone();
        let io_runtime = get_io_runtime(true);
        let handle = io_runtime.spawn_blocking(move || -> DaftResult<W> {
            write_fn(&mut file_writer, &record_batches)?;
            Ok(file_writer)
        });
        let file_writer = handle.await??;
        self.file_writer.replace(file_writer);

        Ok(WriteResult {
            bytes_written: est_bytes_to_write,
            rows_written: num_rows,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(file_writer) = self.file_writer.take()
            && let Some(finish_fn) = self.close_fn.clone()
        {
            let io_runtime = get_io_runtime(true);
            io_runtime
                .spawn_blocking(move || -> DaftResult<()> { finish_fn(file_writer) })
                .await??;
        }

        self.storage_backend.finalize().await?;

        let field = Field::new(RETURN_PATHS_COLUMN_NAME, DataType::Utf8);
        let filename_series = Series::from_arrow(
            Arc::new(field.clone()),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice([&self
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
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written()]
    }
}
