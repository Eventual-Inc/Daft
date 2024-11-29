use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetFileSizeWriter is a writer that writes in files of a target size.
// It rotates the writer when the current file reaches the target size.
struct TargetFileSizeWriter {
    current_file_size_bytes: usize,
    current_writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_file_min_size_bytes: usize,
    target_file_max_size_bytes: usize,
    results: Vec<Table>,
    partition_values: Option<Table>,
    is_closed: bool,
}

impl TargetFileSizeWriter {
    const FILE_SIZE_LENIENCY: f64 = 0.2;
    fn new(
        target_in_memory_file_size_bytes: usize,
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        partition_values: Option<Table>,
    ) -> DaftResult<Self> {
        let writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>> =
            writer_factory.create_writer(0, partition_values.as_ref())?;
        Ok(Self {
            current_file_size_bytes: 0,
            current_writer: writer,
            writer_factory,
            target_file_min_size_bytes: (target_in_memory_file_size_bytes as f64
                * (1.0 - Self::FILE_SIZE_LENIENCY))
                as usize,
            target_file_max_size_bytes: (target_in_memory_file_size_bytes as f64
                * (1.0 + Self::FILE_SIZE_LENIENCY))
                as usize,
            results: vec![],
            partition_values,
            is_closed: false,
        })
    }

    fn rotate_writer(&mut self) -> DaftResult<()> {
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
        let mut input = input;
        loop {
            let input_size_bytes = input.size_bytes()?.expect("Micropartitions should be loaded before writing, so they should have a size in bytes");
            // If the current file size plus the input size is less than the target min size, write the input to the current file and return.
            if self.current_file_size_bytes + input_size_bytes < self.target_file_min_size_bytes {
                self.current_file_size_bytes += input_size_bytes;
                self.current_writer.write(input)?;
                break;
            }
            // If the current file size plus the input size is less than the target max size, write the input to the current file, rotate the writer, and return.
            else if self.current_file_size_bytes + input_size_bytes
                < self.target_file_max_size_bytes
            {
                self.current_file_size_bytes += input_size_bytes;
                self.current_writer.write(input)?;
                self.rotate_writer()?;
                break;
            }
            // If the current file size plus the input size is greater than the target max size, get the remaining bytes that can be written to the current file.
            // Write the remaining bytes to the current file, rotate the writer, and repeat the process with the remaining input.
            else {
                let remaining_bytes =
                    self.target_file_max_size_bytes - self.current_file_size_bytes;
                let avg_row_size = input_size_bytes as f64 / input.len() as f64;
                let remaining_rows = (remaining_bytes as f64 / avg_row_size).floor() as usize;
                let (to_write, remaining) = input.split_at(remaining_rows)?;
                self.current_writer.write(to_write.into())?;
                self.rotate_writer()?;
                if remaining.is_empty() {
                    break;
                } else {
                    input = remaining.into();
                }
            }
        }
        Ok(())
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
    target_in_memory_file_rows: usize,
}

impl TargetFileSizeWriterFactory {
    pub(crate) fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        target_in_memory_file_rows: usize,
    ) -> Self {
        Self {
            writer_factory,
            target_in_memory_file_rows,
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
            self.target_in_memory_file_rows,
            self.writer_factory.clone(),
            partition_values.cloned(),
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
        let mut writer =
            TargetFileSizeWriter::new(1, Arc::new(dummy_writer_factory), None).unwrap();

        let mp = make_dummy_mp(1);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_target_file_writer_less_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let mut writer =
            TargetFileSizeWriter::new(3, Arc::new(dummy_writer_factory), None).unwrap();

        let mp = make_dummy_mp(2);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_target_file_writer_more_rows_for_one_file() {
        let dummy_writer_factory = DummyWriterFactory;
        let mut writer =
            TargetFileSizeWriter::new(3, Arc::new(dummy_writer_factory), None).unwrap();

        let mp = make_dummy_mp(4);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn test_target_file_writer_multiple_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let mut writer =
            TargetFileSizeWriter::new(3, Arc::new(dummy_writer_factory), None).unwrap();

        let mp = make_dummy_mp(10);
        writer.write(mp).unwrap();
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 4);
    }

    #[test]
    fn test_target_file_writer_many_writes_many_files() {
        let dummy_writer_factory = DummyWriterFactory;
        let mut writer =
            TargetFileSizeWriter::new(3, Arc::new(dummy_writer_factory), None).unwrap();

        for _ in 0..10 {
            let mp = make_dummy_mp(1);
            writer.write(mp).unwrap();
        }
        let res = writer.close().unwrap();
        assert_eq!(res.len(), 4);
    }
}
