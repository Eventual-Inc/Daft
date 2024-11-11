use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetBatchWriter is a writer that writes in batches of rows, i.e. for Parquet where we want to write
// a row group at a time.
pub struct TargetBatchWriter {
    target_in_memory_chunk_rows: usize,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    leftovers: Option<Arc<MicroPartition>>,
    is_closed: bool,
}

impl TargetBatchWriter {
    pub fn new(
        target_in_memory_chunk_rows: usize,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            target_in_memory_chunk_rows,
            writer,
            leftovers: None,
            is_closed: false,
        }
    }
}

impl FileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetBatchWriter"
        );
        let input = if let Some(leftovers) = self.leftovers.take() {
            MicroPartition::concat([&leftovers, input])?.into()
        } else {
            input.clone()
        };

        let mut local_offset = 0;
        loop {
            let remaining_rows = input.len() - local_offset;

            use std::cmp::Ordering;
            match remaining_rows.cmp(&self.target_in_memory_chunk_rows) {
                Ordering::Equal => {
                    // Write exactly one chunk
                    let chunk = input.slice(local_offset, local_offset + remaining_rows)?;
                    return self.writer.write(&chunk.into());
                }
                Ordering::Less => {
                    // Store remaining rows as leftovers
                    let remainder = input.slice(local_offset, local_offset + remaining_rows)?;
                    self.leftovers = Some(remainder.into());
                    return Ok(());
                }
                Ordering::Greater => {
                    // Write a complete chunk and continue
                    let chunk = input.slice(
                        local_offset,
                        local_offset + self.target_in_memory_chunk_rows,
                    )?;
                    self.writer.write(&chunk.into())?;
                    local_offset += self.target_in_memory_chunk_rows;
                }
            }
        }
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(leftovers) = self.leftovers.take() {
            self.writer.write(&leftovers)?;
        }
        self.is_closed = true;
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
        writer.write(&mp).unwrap();
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
            writer.write(&mp).unwrap();
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
        writer.write(&mp).unwrap();
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
