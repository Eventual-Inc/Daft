use std::{cmp::max, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetBatchWriter is a writer that writes in batches of size_bytes, i.e. for Parquet where we want to write
// a row group at a time.
pub struct TargetBatchWriter {
    target_in_memory_chunk_size_bytes: usize,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    leftovers: Option<Arc<MicroPartition>>,
    leftover_size_bytes: usize,
    is_closed: bool,
}

impl TargetBatchWriter {
    pub fn new(
        target_in_memory_chunk_size_bytes: usize,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            target_in_memory_chunk_size_bytes,
            writer,
            leftovers: None,
            leftover_size_bytes: 0,
            is_closed: false,
        }
    }
}

impl FileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, mut input: Arc<MicroPartition>) -> DaftResult<()> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetBatchWriter"
        );
        if input.is_empty() {
            return Ok(());
        }
        
        let mut input_size_bytes = input
            .size_bytes()?
            .expect("Micropartitions in target batch writer must be loaded");

        if let Some(leftovers) = self.leftovers.take() {
            input = MicroPartition::concat([leftovers, input])?.into();
            input_size_bytes += self.leftover_size_bytes;
            self.leftover_size_bytes = 0;
        }

        // Calculate the average row size in bytes, and determine the target chunk size in rows
        let avg_row_size_bytes = max(input_size_bytes / input.len(), 1);
        let target_chunk_rows = self.target_in_memory_chunk_size_bytes / avg_row_size_bytes;

        // Write chunks of target_chunk_rows until we have less than that
        let mut local_offset = 0;
        loop {
            let remaining_rows = input.len() - local_offset;

            use std::cmp::Ordering;
            match remaining_rows.cmp(&target_chunk_rows) {
                // We have enough rows to write a complete chunk, write it and return
                Ordering::Equal => {
                    let chunk = input.slice(local_offset, local_offset + remaining_rows)?;
                    return self.writer.write(chunk.into());
                }
                // We have less rows than a chunk, store the remainder and return
                Ordering::Less => {
                    let remainder = input.slice(local_offset, local_offset + remaining_rows)?;
                    let remainder_size_bytes = remainder.len() * avg_row_size_bytes; 
                    self.leftovers = Some(remainder.into());
                    self.leftover_size_bytes = remainder_size_bytes;
                    return Ok(());
                }
                // We have more rows than a chunk, write the chunk, increment the offset and continue
                Ordering::Greater => {
                    let chunk = input.slice(local_offset, local_offset + target_chunk_rows)?;
                    self.writer.write(chunk.into())?;
                    local_offset += target_chunk_rows;
                }
            }
        }
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(leftovers) = self.leftovers.take() {
            self.writer.write(leftovers)?;
        }
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
