use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, TargetInMemorySizeBytesCalculator, WriterFactory};

struct TableWithSize {
    table: Table,
    size_bytes: usize,
}

impl TableWithSize {
    fn new(table: Table) -> DaftResult<Self> {
        let size_bytes = table.size_bytes()?;
        Ok(Self { table, size_bytes })
    }
}

struct SizeBasedBuffer {
    buffer: VecDeque<TableWithSize>,
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
            let table_with_size = TableWithSize::new(table.clone())?;
            self.size_bytes += table_with_size.size_bytes;
            self.buffer.push_back(table_with_size);
        }
        Ok(())
    }

    fn pop(
        &mut self,
        min_bytes: usize,
        max_bytes: usize,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.size_bytes < min_bytes {
            return Ok(None);
        }

        let mut tables = Vec::new();
        let mut bytes_taken = 0;

        while let Some(buffered) = self.buffer.pop_front() {
            if bytes_taken + buffered.size_bytes <= max_bytes {
                // Take whole table
                bytes_taken += buffered.size_bytes;
                tables.push(buffered.table);
                if bytes_taken >= min_bytes {
                    break;
                }
            } else {
                // Need to split table
                let needed_bytes = min_bytes - bytes_taken;
                let rows = buffered.table.len();
                let avg_row_bytes = buffered.size_bytes.checked_div(rows).unwrap_or(1);
                let rows_needed = needed_bytes / avg_row_bytes;

                if rows_needed == 0 {
                    // Can't split further
                    self.buffer.push_front(buffered);
                    break;
                } else if rows_needed >= rows {
                    // Take whole table
                    bytes_taken += buffered.size_bytes;
                    tables.push(buffered.table);
                } else {
                    // Split table
                    let split = buffered.table.slice(0, rows_needed)?;
                    let remainder = buffered.table.slice(rows_needed, rows)?;

                    bytes_taken += avg_row_bytes * rows_needed;
                    tables.push(split);

                    self.buffer.push_front(TableWithSize {
                        table: remainder,
                        size_bytes: avg_row_bytes * (rows - rows_needed),
                    });
                }
                break;
            }
        }

        if tables.is_empty() {
            return Ok(None);
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

        let tables = self.buffer.drain(..).map(|b| b.table).collect::<Vec<_>>();

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

    fn write_and_update_inflation_factor(
        &mut self,
        input: Arc<MicroPartition>,
        in_memory_size_bytes: usize,
    ) -> DaftResult<()> {
        self.writer.write(input)?;
        let current_position = self.writer.tell()?;
        if let Some(current_position) = current_position {
            let actual_size_bytes = current_position - self.previous_position;
            self.previous_position = current_position;
            self.size_calculator
                .record_and_update_inflation_factor(actual_size_bytes, in_memory_size_bytes);
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

        let mut target_size_bytes = self.size_calculator.calculate_target_in_memory_size_bytes();
        let mut min_size_bytes =
            (target_size_bytes as f64 * (1.0 - Self::SIZE_BYTE_LENIENCY)) as usize;
        let mut max_size_bytes =
            (target_size_bytes as f64 * (1.0 + Self::SIZE_BYTE_LENIENCY)) as usize;

        while let Some(mp) = self.buffer.pop(min_size_bytes, max_size_bytes)? {
            self.write_and_update_inflation_factor(mp, target_size_bytes)?;
            target_size_bytes = self.size_calculator.calculate_target_in_memory_size_bytes();
            min_size_bytes = (target_size_bytes as f64 * (1.0 - Self::SIZE_BYTE_LENIENCY)) as usize;
            max_size_bytes = (target_size_bytes as f64 * (1.0 + Self::SIZE_BYTE_LENIENCY)) as usize;
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
