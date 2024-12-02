use std::{cmp::max, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, TargetInMemorySizeBytesCalculator, WriterFactory};

// TargetFileSizeWriter is a writer that writes files of a target size.
// It rotates the writer when the current file reaches the target size.
struct TargetFileSizeWriter {
    current_file_size_bytes: usize,
    current_writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    results: Vec<Table>,
    partition_values: Option<Table>,
    is_closed: bool,
}

impl TargetFileSizeWriter {
    fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        partition_values: Option<Table>,
        size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
    ) -> DaftResult<Self> {
        let writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>> =
            writer_factory.create_writer(0, partition_values.as_ref())?;
        Ok(Self {
            current_file_size_bytes: 0,
            current_writer: writer,
            writer_factory,
            size_calculator,
            results: vec![],
            partition_values,
            is_closed: false,
        })
    }

    fn rotate_writer(&mut self) -> DaftResult<()> {
        if let Some(bytes) = self.current_writer.tell()? {
            self.size_calculator
                .record_and_update_inflation_factor(bytes);
        }
        if let Some(result) = self.current_writer.close()? {
            self.results.push(result);
        }
        self.current_file_size_bytes = 0;
        self.current_writer = self
            .writer_factory
            .create_writer(self.results.len(), self.partition_values.as_ref())?;
        Ok(())
    }
}

impl FileWriter for TargetFileSizeWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<Table>;

    fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed TargetFileSizeWriter"
        );
        use std::cmp::Ordering;
        if input.is_empty() {
            return Ok(());
        }

        let input_size_bytes = input.size_bytes()?.expect(
            "Micropartitions should be loaded before writing, so they should have a size in bytes",
        );
        let avg_row_size_bytes = max(input_size_bytes / input.len(), 1);

        // Write the input, rotating the writer when the current file reaches the target size
        let mut local_offset = 0;
        loop {
            let bytes_until_target = self
                .size_calculator
                .calculate_target_in_memory_size_bytes()
                .saturating_sub(self.current_file_size_bytes);
            let rows_until_target = max(bytes_until_target / avg_row_size_bytes, 1);
            let remaining_input_rows = input.len() - local_offset;
            match remaining_input_rows.cmp(&rows_until_target) {
                // We have enough rows to finish the file, write it, rotate the writer and return
                Ordering::Equal => {
                    let to_write =
                        input.slice(local_offset, local_offset + remaining_input_rows)?;
                    self.current_writer.write(to_write.into())?;
                    self.rotate_writer()?;
                    self.current_file_size_bytes = 0;
                    return Ok(());
                }
                // We have less rows than the target, write them and return
                Ordering::Less => {
                    let to_write =
                        input.slice(local_offset, local_offset + remaining_input_rows)?;
                    self.current_writer.write(to_write.into())?;
                    self.current_file_size_bytes += remaining_input_rows * avg_row_size_bytes;
                    return Ok(());
                }
                // We have more rows to write, write the target amount, rotate the writer and continue
                Ordering::Greater => {
                    let to_write = input.slice(local_offset, local_offset + rows_until_target)?;
                    self.current_writer.write(to_write.into())?;
                    self.rotate_writer()?;
                    self.current_file_size_bytes = 0;
                    local_offset += rows_until_target;
                }
            }
        }
    }

    fn tell(&self) -> DaftResult<Option<usize>> {
        self.current_writer.tell()
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if self.current_file_size_bytes > 0 {
            if let Some(result) = self.current_writer.close()? {
                self.results.push(result);
            }
        }
        self.is_closed = true;
        Ok(std::mem::take(&mut self.results))
    }
}

pub(crate) struct TargetFileSizeWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    size_calculator: Arc<TargetInMemorySizeBytesCalculator>,
}

impl TargetFileSizeWriterFactory {
    pub(crate) fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
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
    type Result = Vec<Table>;

    fn create_writer(
        &self,
        _file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(TargetFileSizeWriter::new(
            self.writer_factory.clone(),
            partition_values.cloned(),
            self.size_calculator.clone(),
        )?)
            as Box<
                dyn FileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test::{make_dummy_mp, DummyWriterFactory};

    #[test]
    fn test_target_file_writer_exact_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(1, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(1);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_target_file_writer_less_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(2);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_target_file_writer_more_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(4);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn test_target_file_writer_multiple_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        let mp = make_dummy_mp(10);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 4);
    }

    #[test]
    fn test_target_file_writer_many_writes_many_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let size_calculator = Arc::new(TargetInMemorySizeBytesCalculator::new(3, 1.0));
        let mut writer =
            TargetFileSizeWriter::new(Arc::new(dummy_writer_factory), None, size_calculator)
                .unwrap();

        for _ in 0..10 {
            let mp = make_dummy_mp(1);
            writer.write(mp).unwrap();
        }
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 4);
    }
}
