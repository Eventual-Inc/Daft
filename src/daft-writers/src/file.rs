use std::{cmp::max, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, TargetInMemorySizeBytesCalculator, WriterFactory};

// TargetFileSizeWriter is a writer that writes files of a target size.
// It rotates the writer when the current file reaches the target size.
struct TargetFileSizeWriter {
    current_in_memory_size_estimate: usize,
    current_in_memory_bytes_written: usize,
    total_physical_bytes_written: usize,
    bytes_per_file: Vec<usize>,
    current_writer:
        Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    writer_factory:
        Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    results: Vec<RecordBatch>,
    partition_values: Option<RecordBatch>,
    is_closed: bool,
}

impl TargetFileSizeWriter {
    fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>,
        >,
        partition_values: Option<RecordBatch>,
        size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    ) -> DaftResult<Self> {
        let writer: Box<
            dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>,
        > = writer_factory.create_writer(0, partition_values.as_ref())?;
        let estimate = size_calculator.calculate_target_in_memory_size_bytes();
        Ok(Self {
            current_in_memory_size_estimate: estimate,
            current_in_memory_bytes_written: 0,
            total_physical_bytes_written: 0,
            bytes_per_file: vec![],
            current_writer: writer,
            writer_factory,
            size_calculator,
            results: vec![],
            partition_values,
            is_closed: false,
        })
    }

    fn remaining_bytes_for_current_file(&self) -> usize {
        self.current_in_memory_size_estimate
            .saturating_sub(self.current_in_memory_bytes_written)
    }

    async fn write_and_update_bytes(
        &mut self,
        input: Arc<MicroPartition>,
        size_bytes: usize,
    ) -> DaftResult<usize> {
        self.current_in_memory_bytes_written += size_bytes;
        let written_bytes = self.current_writer.write(input).await?;
        self.total_physical_bytes_written += written_bytes;
        if self.current_in_memory_bytes_written >= self.current_in_memory_size_estimate {
            self.rotate_writer_and_update_estimates().await?;
        }
        Ok(written_bytes)
    }

    async fn rotate_writer_and_update_estimates(&mut self) -> DaftResult<()> {
        // Record the size of the current file and update the inflation factor
        self.size_calculator.record_and_update_inflation_factor(
            self.current_writer.bytes_written(),
            self.current_in_memory_bytes_written,
        );
        // Update the target size estimate
        self.current_in_memory_size_estimate =
            self.size_calculator.calculate_target_in_memory_size_bytes();

        // Close the current writer and add the result to the results
        if let Some(result) = self.current_writer.close().await? {
            self.results.push(result);
            self.bytes_per_file
                .push(self.current_writer.bytes_written());
        }

        // Create a new writer and reset the current bytes written
        self.current_in_memory_bytes_written = 0;
        self.current_writer = self
            .writer_factory
            .create_writer(self.results.len(), self.partition_values.as_ref())?;
        Ok(())
    }
}

#[async_trait]
impl AsyncFileWriter for TargetFileSizeWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<usize> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetFileSizeWriter"
        );
        use std::cmp::Ordering;
        if input.is_empty() {
            return Ok(0);
        }

        let input_size_bytes = input.size_bytes()?.expect(
            "Micropartitions should be loaded before writing, so they should have a size in bytes",
        );
        let avg_row_size_bytes = max(input_size_bytes / input.len(), 1);
        // Write the input, rotating the writer when the current file reaches the target size
        let mut local_offset = 0;
        let mut bytes_written = 0;
        loop {
            let rows_until_target = max(
                self.remaining_bytes_for_current_file() / avg_row_size_bytes,
                1,
            );
            let remaining_input_rows = input.len() - local_offset;
            match remaining_input_rows.cmp(&rows_until_target) {
                // We have enough rows to finish the file, write it, rotate the writer and return
                Ordering::Equal | Ordering::Less => {
                    let to_write =
                        input.slice(local_offset, local_offset + remaining_input_rows)?;
                    bytes_written += self
                        .write_and_update_bytes(
                            to_write.into(),
                            remaining_input_rows * avg_row_size_bytes,
                        )
                        .await?;
                    return Ok(bytes_written);
                }
                // We have more rows to write, write the target amount, rotate the writer and continue
                Ordering::Greater => {
                    let to_write = input.slice(local_offset, local_offset + rows_until_target)?;
                    self.write_and_update_bytes(
                        to_write.into(),
                        rows_until_target * avg_row_size_bytes,
                    )
                    .await?;
                    local_offset += rows_until_target;
                }
            }
        }
    }

    fn bytes_written(&self) -> usize {
        self.total_physical_bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.bytes_per_file.clone()
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if self.current_in_memory_bytes_written > 0 {
            if let Some(result) = self.current_writer.close().await? {
                self.results.push(result);
                self.bytes_per_file
                    .push(self.current_writer.bytes_written());
            }
        }
        self.is_closed = true;
        Ok(std::mem::take(&mut self.results))
    }
}

pub(crate) struct TargetFileSizeWriterFactory {
    writer_factory:
        Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
}

impl TargetFileSizeWriterFactory {
    pub(crate) fn new(
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

impl WriterFactory for TargetFileSizeWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        _file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(TargetFileSizeWriter::new(
            self.writer_factory.clone(),
            partition_values.cloned(),
            self.size_calculator.clone(),
        )?)
            as Box<
                dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test::{make_dummy_mp, DummyWriterFactory};

    #[tokio::test]
    async fn test_target_file_writer_exact_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(1, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(1);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();
        assert_eq!(res.len(), 1);
    }

    #[tokio::test]
    async fn test_target_file_writer_less_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(2);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();
        assert_eq!(res.len(), 1);
    }

    #[tokio::test]
    async fn test_target_file_writer_more_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(4);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();
        assert_eq!(res.len(), 2);
    }

    #[tokio::test]
    async fn test_target_file_writer_multiple_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(10);
        writer.write(mp).await.unwrap();
        let res = writer.close().await.unwrap();
        assert_eq!(res.len(), 4);
    }

    #[tokio::test]
    async fn test_target_file_writer_many_writes_many_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        for _ in 0..10 {
            let mp = make_dummy_mp(1);
            writer.write(mp).await.unwrap();
        }
        let res = writer.close().await.unwrap();
        assert_eq!(res.len(), 4);
    }
}
