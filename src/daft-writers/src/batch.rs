use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetBatchWriter is a writer that writes in batches of size_bytes, i.e. for Parquet where we want to write
// a row group at a time.
pub struct TargetBatchWriter {
    target_chunk_min_size_bytes: usize,
    target_chunk_max_size_bytes: usize,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    current_buffer: Vec<Table>,
    current_buffer_size_bytes: usize,
    is_closed: bool,
}

impl TargetBatchWriter {
    const CHUNK_SIZE_LENIENCY: f64 = 0.2;

    pub fn new(
        target_in_memory_chunk_size_bytes: usize,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            target_chunk_min_size_bytes: (target_in_memory_chunk_size_bytes as f64
                * (1.0 - Self::CHUNK_SIZE_LENIENCY))
                as usize,
            target_chunk_max_size_bytes: (target_in_memory_chunk_size_bytes as f64
                * (1.0 + Self::CHUNK_SIZE_LENIENCY))
                as usize,
            writer,
            current_buffer: vec![],
            current_buffer_size_bytes: 0,
            is_closed: false,
        }
    }

    fn write_buffer(&mut self) -> DaftResult<()> {
        if !self.current_buffer.is_empty() {
            let to_write = std::mem::take(&mut self.current_buffer);
            let mp = MicroPartition::new_loaded(
                to_write.first().unwrap().schema.clone(),
                to_write.into(),
                None,
            );
            self.writer.write(Arc::new(mp))?;
            self.current_buffer_size_bytes = 0;
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

        let input_tables = input.get_tables()?;
        let mut input_tables_iter = input_tables.iter();
        let mut next_table = input_tables_iter.next().cloned();

        while let Some(table) = next_table {
            let table_size_bytes = table.size_bytes()?;
            if self.current_buffer_size_bytes + table_size_bytes < self.target_chunk_min_size_bytes
            {
                self.current_buffer.push(table);
                self.current_buffer_size_bytes += table_size_bytes;
                next_table = input_tables_iter.next().cloned();
            } else if self.current_buffer_size_bytes + table_size_bytes
                < self.target_chunk_max_size_bytes
            {
                self.current_buffer.push(table);
                self.write_buffer()?;
                next_table = input_tables_iter.next().cloned();
            } else {
                let avg_row_size = table_size_bytes as f64 / table.len() as f64;
                let rows_needed_to_fill = ((self.target_chunk_max_size_bytes as f64
                    - self.current_buffer_size_bytes as f64)
                    / avg_row_size)
                    .floor() as usize;
                let to_write = table.slice(0, rows_needed_to_fill)?;
                self.current_buffer.push(to_write.clone());
                self.write_buffer()?;
                next_table = Some(table.slice(rows_needed_to_fill, table.len())?);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        self.write_buffer()?;
        self.is_closed = true;
        self.writer.close()
    }
}

pub struct TargetBatchWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_batch_size_bytes: usize,
}

impl TargetBatchWriterFactory {
    pub fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        target_batch_size_bytes: usize,
    ) -> Self {
        Self {
            writer_factory,
            target_batch_size_bytes,
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
            self.target_batch_size_bytes,
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
        let mut writer =
            TargetBatchWriter::new(1, dummy_writer_factory.create_writer(0, None).unwrap());

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
        let mut writer =
            TargetBatchWriter::new(3, dummy_writer_factory.create_writer(0, None).unwrap());

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
        let mut writer =
            TargetBatchWriter::new(3, dummy_writer_factory.create_writer(0, None).unwrap());

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
