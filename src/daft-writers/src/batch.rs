use std::sync::Arc;

use common_buffer::RowBasedBuffer;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetBatchWriter is a writer that writes in batches of rows, i.e. for Parquet where we want to write
// a row group at a time. It uses a buffer to accumulate rows until it has enough to write a batch.
pub struct TargetBatchWriter {
    buffer: RowBasedBuffer<Arc<MicroPartition>>,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
}

impl TargetBatchWriter {
    pub fn new(
        target_in_memory_chunk_rows: usize,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            buffer: RowBasedBuffer::new(target_in_memory_chunk_rows),
            writer,
        }
    }
}

impl FileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
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

pub struct TargetBatchWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_in_memory_chunk_rows: usize,
}

impl TargetBatchWriterFactory {
    pub fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        target_in_memory_chunk_rows: usize,
    ) -> Self {
        Self {
            writer_factory,
            target_in_memory_chunk_rows,
        }
    }
}

impl WriterFactory for TargetBatchWriterFactory {
    type Input = Arc<MicroPartition>;
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
