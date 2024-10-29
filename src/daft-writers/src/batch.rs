use std::sync::Arc;

use common_buffer::{Bufferable, RowBasedBuffer};
use common_error::DaftResult;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetBatchWriter is a writer that writes in batches of rows, i.e. for Parquet where we want to write
// a row group at a time. It uses a buffer to accumulate rows until it has enough to write a batch.
pub struct TargetBatchWriter<B: Bufferable> {
    buffer: RowBasedBuffer<B>,
    writer: Box<dyn FileWriter<Input = B, Result = Option<Table>>>,
}

impl<B: Bufferable> TargetBatchWriter<B> {
    pub fn new(
        target_in_memory_chunk_rows: usize,
        writer: Box<dyn FileWriter<Input = B, Result = Option<Table>>>,
    ) -> Self {
        Self {
            buffer: RowBasedBuffer::new(target_in_memory_chunk_rows),
            writer,
        }
    }
}

impl<B: Bufferable + Send + Sync + Clone> FileWriter for TargetBatchWriter<B> {
    type Input = B;
    type Result = Option<Table>;

    fn write(&mut self, input: &B) -> DaftResult<()> {
        self.buffer.push(input.clone());
        if let Some(ready) = self.buffer.pop_enough()? {
            for r in ready {
                self.writer.write(&r)?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(ready) = self.buffer.pop_all()? {
            self.writer.write(&ready)?;
        }
        self.writer.close()
    }
}

pub struct TargetBatchWriterFactory<B: Bufferable> {
    writer_factory: Arc<dyn WriterFactory<Input = B, Result = Option<Table>>>,
    target_in_memory_chunk_rows: usize,
}

impl<B: Bufferable> TargetBatchWriterFactory<B> {
    pub fn new(
        writer_factory: Arc<dyn WriterFactory<Input = B, Result = Option<Table>>>,
        target_in_memory_chunk_rows: usize,
    ) -> Self {
        Self {
            writer_factory,
            target_in_memory_chunk_rows,
        }
    }
}

impl<B: Bufferable + Send + Sync + Clone + 'static> WriterFactory for TargetBatchWriterFactory<B> {
    type Input = B;
    type Result = Option<Table>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = self
            .writer_factory
            .create_writer(file_idx, partition_values)?;
        Ok(Box::new(TargetBatchWriter::new(
            self.target_in_memory_chunk_rows,
            writer,
        )))
    }
}
