use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, TargetInMemorySizeBytesCalculator, WriterFactory};

struct Entry {
    table: Table,
    size_bytes: usize,
}

struct SizeBasedBuffer {
    buffer: VecDeque<Entry>,
    size_bytes: usize,
    num_rows: usize,
}

impl SizeBasedBuffer {
    fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
            size_bytes: 0,
            num_rows: 0,
        }
    }

    fn push(&mut self, mp: Arc<MicroPartition>) -> DaftResult<()> {
        let tables = mp.get_tables()?;
        for table in tables.iter() {
            let size_bytes = table.size_bytes()?;
            self.size_bytes += size_bytes;
            self.num_rows += table.len();
            self.buffer.push_back(Entry {
                table: table.clone(),
                size_bytes,
            });
        }
        Ok(())
    }

    fn pop(
        &mut self,
        min_size_bytes: usize,
        max_size_bytes: usize,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.size_bytes < min_size_bytes {
            return Ok(None);
        }
        let mut tables = vec![];
        let mut current_size_bytes = 0;
        let mut current_num_rows = 0;
        while let Some(entry) = self.buffer.pop_front() {
            if current_size_bytes + entry.size_bytes < max_size_bytes {
                current_size_bytes += entry.size_bytes;
                current_num_rows += entry.table.len();
                tables.push(entry.table);
                if current_size_bytes >= min_size_bytes {
                    break;
                }
            } else {
                let entry_table_len = entry.table.len();
                let avg_row_size_bytes = entry.size_bytes / entry_table_len;
                let size_bytes_needed = min_size_bytes - current_size_bytes;
                let num_rows_needed = size_bytes_needed / avg_row_size_bytes;

                let sliced = entry.table.slice(0, num_rows_needed)?;
                tables.push(sliced);

                self.buffer.push_front(Entry {
                    table: entry.table.slice(num_rows_needed, entry_table_len)?,
                    size_bytes: avg_row_size_bytes * (entry_table_len - num_rows_needed),
                });
                current_size_bytes += size_bytes_needed;
                current_num_rows += num_rows_needed;
                break;
            }
        }
        self.size_bytes -= current_size_bytes;
        self.num_rows -= current_num_rows;
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
            .map(|entry| entry.table)
            .collect::<Vec<_>>();
        self.size_bytes = 0;
        self.num_rows = 0;
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
    previous_position: usize,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    buffer: SizeBasedBuffer,
    is_closed: bool,
}

impl TargetBatchWriter {
    const SIZE_BYTE_LENIENCY: f64 = 0.2;

    pub fn new(
        size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            size_calculator,
            previous_position: 0,
            writer,
            buffer: SizeBasedBuffer::new(),
            is_closed: false,
        }
    }

    fn write_and_update_inflation_factor(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        self.writer.write(input)?;
        let current_position = self.writer.tell()?;
        if let Some(current_position) = current_position {
            let actual_size_bytes = current_position - self.previous_position;
            self.previous_position = current_position;
            self.size_calculator
                .record_and_update_inflation_factor(actual_size_bytes);
        }
        Ok(())
    }
}

impl FileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetBatchWriter"
        );
        if input.is_empty() {
            return Ok(());
        }

        self.buffer.push(input)?;

        let target_size_bytes = self.size_calculator.calculate_target_in_memory_size_bytes();
        let min_size_bytes = (target_size_bytes as f64 * (1.0 - Self::SIZE_BYTE_LENIENCY)) as usize;
        let max_size_bytes = (target_size_bytes as f64 * (1.0 + Self::SIZE_BYTE_LENIENCY)) as usize;

        while let Some(mp) = self.buffer.pop(min_size_bytes, max_size_bytes)? {
            self.write_and_update_inflation_factor(mp)?;
        }

        Ok(())
    }

    fn tell(&self) -> DaftResult<Option<usize>> {
        self.writer.tell()
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(leftovers) = self.buffer.pop_all()? {
            self.writer.write(leftovers)?;
        }
        self.is_closed = true;
        self.writer.close()
    }
}

pub struct TargetBatchWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
}

impl TargetBatchWriterFactory {
    pub fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
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
            self.size_calculator.clone(),
            writer,
        )))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test::{make_dummy_mp, DummyWriterFactory};

    #[test]
    fn test_target_batch_writer_exact_batch() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(1, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        let mp = make_dummy_mp(1);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();

        assert!(res.is_some());
        let write_count = res
            .unwrap()
            .get_column("write_count")
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 1);
    }

    #[test]
    fn test_target_batch_writer_small_batches() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        for _ in 0..8 {
            let mp = make_dummy_mp(1);
            writer.write(mp).unwrap();
        }
        let res = writer.close().unwrap();

        assert!(res.is_some());
        let write_count = res
            .unwrap()
            .get_column("write_count")
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 3);
    }

    #[test]
    fn test_target_batch_writer_big_batch() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer = TargetBatchWriter::new(
            size_calculator,
            dummy_writer_factory.create_writer(0, None).unwrap(),
        );

        let mp = make_dummy_mp(10);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();

        assert!(res.is_some());
        let write_count = res
            .unwrap()
            .get_column("write_count")
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(write_count, 4);
    }
}
