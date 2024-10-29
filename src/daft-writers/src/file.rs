use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

// TargetFileSizeWriter is a writer that writes in files of a target size.
// It rotates the writer when the current file reaches the target size.
struct TargetFileSizeWriter {
    current_file_rows: usize,
    current_writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_in_memory_file_rows: usize,
    results: Vec<Table>,
    partition_values: Option<Table>,
}

impl TargetFileSizeWriter {
    fn new(
        target_in_memory_file_rows: usize,
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
        partition_values: Option<Table>,
    ) -> DaftResult<Self> {
        let writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>> =
            writer_factory.create_writer(0, partition_values.as_ref())?;
        Ok(Self {
            current_file_rows: 0,
            current_writer: writer,
            writer_factory,
            target_in_memory_file_rows,
            results: vec![],
            partition_values,
        })
    }

    fn rotate_writer(&mut self) -> DaftResult<()> {
        if let Some(result) = self.current_writer.close()? {
            self.results.push(result);
        }
        self.current_file_rows = 0;
        self.current_writer = self
            .writer_factory
            .create_writer(self.results.len(), self.partition_values.as_ref())?;
        Ok(())
    }
}

impl FileWriter for TargetFileSizeWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<Table>;

    fn write(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        use std::cmp::Ordering;
        match (input.len() + self.current_file_rows).cmp(&self.target_in_memory_file_rows) {
            Ordering::Equal => {
                self.current_writer.write(input)?;
                self.rotate_writer()?;
            }
            Ordering::Greater => {
                // Finish up the current writer first
                let remaining_rows = self.target_in_memory_file_rows - self.current_file_rows;
                let (to_write, mut remaining) = input.split_at(remaining_rows)?;
                self.current_writer.write(&to_write.into())?;
                self.rotate_writer()?;

                // Write as many full files as possible
                let num_full_files = remaining.len() / self.target_in_memory_file_rows;
                for _ in 0..num_full_files {
                    let (to_write, new_remaining) =
                        remaining.split_at(self.target_in_memory_file_rows)?;
                    self.current_writer.write(&to_write.into())?;
                    self.rotate_writer()?;
                    remaining = new_remaining;
                }

                // Write the remaining rows
                if !remaining.is_empty() {
                    self.current_file_rows = remaining.len();
                    self.current_writer.write(&remaining.into())?;
                } else {
                    self.current_file_rows = 0;
                }
            }
            Ordering::Less => {
                self.current_writer.write(input)?;
                self.current_file_rows += input.len();
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if self.current_file_rows > 0 {
            if let Some(result) = self.current_writer.close()? {
                self.results.push(result);
            }
        }
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
