use std::{
    cmp::{max, min},
    collections::VecDeque,
    sync::Arc,
};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, TargetInMemorySizeBytesCalculator, WriterFactory};

// SizeBasedBuffer is a buffer that stores tables and their sizes in bytes.
// It produces Micropartitions that are within a certain size range.
// Having a size range instead of exact size allows for more flexibility in the size of the produced Micropartitions,
// and reducing the amount of '.slice' operations.
struct SizeBasedBuffer {
    buffer: VecDeque<(RecordBatch, usize)>,
    size_bytes: usize,
}

impl SizeBasedBuffer {
    fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
            size_bytes: 0,
        }
    }

    fn push(&mut self, mp: Arc<MicroPartition>) -> DaftResult<()> {
        for table in mp.get_tables()?.iter() {
            let size_bytes = table.size_bytes()?;
            self.size_bytes += size_bytes;
            self.buffer.push_back((table.clone(), size_bytes));
        }
        Ok(())
    }

    // Pop tables from the buffer we've popped between min_bytes and max_bytes.
    fn pop(
        &mut self,
        min_bytes: usize,
        max_bytes: usize,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        assert!(min_bytes <= max_bytes);
        if self.size_bytes < min_bytes || self.buffer.is_empty() {
            return Ok(None);
        }

        let mut tables = Vec::new();
        let mut bytes_taken = 0;

        while let Some((table, size)) = self.buffer.pop_front() {
            // If we can fit the table in the current batch, add it and continue
            if bytes_taken + size < min_bytes {
                bytes_taken += size;
                tables.push(table);
            }
            // Else if we have enough tables to fill the batch, break
            else if bytes_taken + size <= max_bytes {
                bytes_taken += size;
                tables.push(table);
                break;
            }
            // Else, we have a table that is too big for the batch, take enough rows to fill the batch, and put the rest back
            // in the buffer.
            else {
                let rows = table.len();
                let avg_row_bytes = max(size / rows, 1);
                let remaining_to_min = min_bytes - bytes_taken;
                let rows_to_take = min(
                    remaining_to_min.div_ceil(avg_row_bytes), // Round up to ensure we hit min_bytes
                    rows,
                );

                let split = table.slice(0, rows_to_take)?;
                let remainder = table.slice(rows_to_take, rows)?;

                bytes_taken += avg_row_bytes * rows_to_take;
                tables.push(split);

                if !remainder.is_empty() {
                    let remainder_size = remainder.len() * avg_row_bytes;
                    self.buffer.push_front((remainder, remainder_size));
                }
                break;
            }
        }

        self.size_bytes -= bytes_taken;

        Ok(Some(Arc::new(MicroPartition::new_loaded(
            tables[0].schema.clone(),
            tables.into(),
            None,
        ))))
    }

    fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let tables = self
            .buffer
            .drain(..)
            .map(|(table, _)| table)
            .collect::<Vec<_>>();

        self.size_bytes = 0;

        Ok(Some(Arc::new(MicroPartition::new_loaded(
            tables[0].schema.clone(),
            tables.into(),
            None,
        ))))
    }
}

// TargetBatchWriter is a writer that writes in batches of size_bytes, i.e. for Parquet where we want to write
// a row group at a time.
pub struct TargetBatchWriter {
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    buffer: SizeBasedBuffer,
    is_closed: bool,
}

impl TargetBatchWriter {
    // The leniency factor for the size of the batch. This is used to allow for some flexibility in the size of the batch,
    // so that we don't have to split tables too often.
    const SIZE_BYTE_LENIENCY: f64 = 0.2;

    pub fn new(
        size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
        writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    ) -> Self {
        Self {
            size_calculator,
            writer,
            buffer: SizeBasedBuffer::new(),
            is_closed: false,
        }
    }

    async fn write_and_update_inflation_factor(
        &mut self,
        input: Arc<MicroPartition>,
        in_memory_size_bytes: usize,
    ) -> DaftResult<usize> {
        let bytes_written = self.writer.write(input).await?;
        self.size_calculator
            .record_and_update_inflation_factor(bytes_written, in_memory_size_bytes);
        Ok(bytes_written)
    }
}

#[async_trait]
impl AsyncFileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<usize> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetBatchWriter"
        );
        if input.is_empty() {
            return Ok(0);
        }

        self.buffer.push(input)?;

        let mut target_size_bytes = self.size_calculator.calculate_target_in_memory_size_bytes();
        let mut min_size_bytes =
            (target_size_bytes as f64 * (1.0 - Self::SIZE_BYTE_LENIENCY)) as usize;
        let mut max_size_bytes =
            (target_size_bytes as f64 * (1.0 + Self::SIZE_BYTE_LENIENCY)) as usize;
        let mut bytes_written = 0;
        while let Some(mp) = self.buffer.pop(min_size_bytes, max_size_bytes)? {
            bytes_written += self
                .write_and_update_inflation_factor(mp, target_size_bytes)
                .await?;
            target_size_bytes = self.size_calculator.calculate_target_in_memory_size_bytes();
            min_size_bytes = (target_size_bytes as f64 * (1.0 - Self::SIZE_BYTE_LENIENCY)) as usize;
            max_size_bytes = (target_size_bytes as f64 * (1.0 + Self::SIZE_BYTE_LENIENCY)) as usize;
        }

        Ok(bytes_written)
    }

    fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.writer.bytes_per_file()
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(leftovers) = self.buffer.pop_all()? {
            self.writer.write(leftovers).await?;
        }
        self.is_closed = true;
        self.writer.close().await
    }
}

pub struct TargetBatchWriterFactory {
    writer_factory:
        Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
}

impl TargetBatchWriterFactory {
    pub fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>,
        >,
        size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    ) -> Self {
        Self {
            writer_factory,
            size_calculator,
        }
    }
}

impl WriterFactory for TargetBatchWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = self
            .writer_factory
            .create_writer(file_idx, partition_values)?;
        Ok(Box::new(TargetBatchWriter::new(
            self.size_calculator.clone(),
            writer,
        )))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test::{make_dummy_mp, DummyWriterFactory};

    #[tokio::test]
    async fn test_target_batch_writer_exact_batch() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(1, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        let mp = make_dummy_mp(1);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();

        assert!(res.is_some());
        let res = res.unwrap();
        let write_count_index = res.schema.get_index("write_count").unwrap();

        let write_count = res
            .get_column(write_count_index)
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 1);
    }

    #[tokio::test]
    async fn test_target_batch_writer_small_batches() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        for _ in 0..8 {
            let mp = make_dummy_mp(1);
            writer.write(mp).await.unwrap();
        }
        let res = writer.close().await.unwrap();

        assert!(res.is_some());
        let res = res.unwrap();
        let write_count_index = res.schema.get_index("write_count").unwrap();

        let write_count = res
            .get_column(write_count_index)
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 3);
    }

    #[tokio::test]
    async fn test_target_batch_writer_big_batch() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        let mp = make_dummy_mp(10);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();

        assert!(res.is_some());
        let res = res.unwrap();
        let write_count_index = res.schema.get_index("write_count").unwrap();

        let write_count = res
            .get_column(write_count_index)
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 4);
    }
}
