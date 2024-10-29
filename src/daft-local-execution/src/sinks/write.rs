use std::{
    cmp::min,
    collections::{hash_map::RawEntryMut, HashMap},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_core::{
    prelude::{AsArrow, SchemaRef},
    utils::identity_hash_set::IndexHash,
};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::OutputFileInfo;
use daft_table::Table;
use daft_writers::{FileWriter, PhysicalWriterFactory, WriterFactory};
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
use crate::{buffer::RowBasedBuffer, pipeline::PipelineResultType};

// TargetBatchWriter is a writer that writes in batches of rows, i.e. for Parquet where we want to write
// a row group at a time. It uses a buffer to accumulate rows until it has enough to write a batch.
struct TargetBatchWriter {
    buffer: RowBasedBuffer,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
}

impl TargetBatchWriter {
    pub fn new(
        target_in_memory_chunk_rows: usize,
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    ) -> Self {
        Self {
            buffer: RowBasedBuffer::new(target_in_memory_chunk_rows),
            writer,
        }
    }
}

impl FileWriter for TargetBatchWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        self.buffer.push(input.clone());
        if let Some(ready) = self.buffer.pop_enough()? {
            for r in ready {
                self.writer.write(&r)?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(ready) = self.buffer.pop_all()? {
            self.writer.write(&ready)?;
        }
        self.writer.close()
    }
}

struct TargetBatchWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_in_memory_chunk_rows: usize,
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
        ))
            as Box<
                dyn FileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

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
    pub fn new(
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

struct TargetFileSizeWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<Table>>>,
    target_in_memory_file_rows: usize,
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

/// PartitionedWriter is a writer that partitions the input data by a set of columns, and writes each partition
/// to a separate file. It uses a map to keep track of the writers for each partition.
struct PartitionedWriter {
    per_partition_writers:
        HashMap<IndexHash, Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<Table>>>>,
    saved_partition_values: Vec<Table>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    partition_by: Vec<ExprRef>,
}

impl PartitionedWriter {
    pub fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
        partition_by: Vec<ExprRef>,
    ) -> Self {
        Self {
            per_partition_writers: HashMap::new(),
            saved_partition_values: vec![],
            writer_factory,
            partition_by,
        }
    }

    fn partition(
        partition_cols: &[ExprRef],
        data: &Arc<MicroPartition>,
    ) -> DaftResult<(Vec<Table>, Table)> {
        let data = data.concat_or_get(IOStatsContext::new("MicroPartition::partition_by_value"))?;
        let table = data.first().unwrap();
        let (split_tables, partition_values) = table.partition_by_value(partition_cols)?;
        Ok((split_tables, partition_values))
    }
}

impl FileWriter for PartitionedWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<Table>;

    fn write(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        let (split_tables, partition_values) =
            Self::partition(self.partition_by.as_slice(), input)?;
        let partition_values_hash = partition_values.hash_rows()?;
        for (idx, (table, partition_value_hash)) in split_tables
            .into_iter()
            .zip(partition_values_hash.as_arrow().values_iter())
            .enumerate()
        {
            let partition_value_row = partition_values.slice(idx, idx + 1)?;
            let entry = self.per_partition_writers.raw_entry_mut().from_hash(
                *partition_value_hash,
                |other| {
                    (*partition_value_hash == other.hash) && {
                        let other_table =
                            self.saved_partition_values.get(other.idx as usize).unwrap();
                        other_table == &partition_value_row
                    }
                },
            );
            match entry {
                RawEntryMut::Vacant(entry) => {
                    let mut writer = self
                        .writer_factory
                        .create_writer(0, Some(partition_value_row.as_ref()))?;
                    writer.write(&Arc::new(MicroPartition::new_loaded(
                        table.schema.clone(),
                        vec![table].into(),
                        None,
                    )))?;
                    entry.insert_hashed_nocheck(
                        *partition_value_hash,
                        IndexHash {
                            idx: self.saved_partition_values.len() as u64,
                            hash: *partition_value_hash,
                        },
                        writer,
                    );
                    self.saved_partition_values.push(partition_value_row);
                }
                RawEntryMut::Occupied(mut entry) => {
                    let writer = entry.get_mut();
                    writer.write(&Arc::new(MicroPartition::new_loaded(
                        table.schema.clone(),
                        vec![table].into(),
                        None,
                    )))?;
                }
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        let mut results = vec![];
        for (_, mut writer) in self.per_partition_writers.drain() {
            results.extend(writer.close()?);
        }
        Ok(results)
    }
}

pub struct WriteSink {
    name: &'static str,
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub fn new(
        file_info: OutputFileInfo,
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> DaftResult<Self> {
        let estimated_row_size_bytes = data_schema.estimate_row_size_bytes();
        let base_writer_factory = PhysicalWriterFactory::new(file_info.clone());
        let (writer, name) = match file_info.file_format {
            FileFormat::Parquet => {
                let target_in_memory_file_size =
                    cfg.parquet_target_filesize as f64 * cfg.parquet_inflation_factor;
                let target_in_memory_row_group_size =
                    cfg.parquet_target_row_group_size as f64 * cfg.parquet_inflation_factor;

                let target_file_rows = if estimated_row_size_bytes > 0.0 {
                    target_in_memory_file_size / estimated_row_size_bytes
                } else {
                    target_in_memory_file_size
                } as usize;

                let target_row_group_rows = min(
                    target_file_rows,
                    if estimated_row_size_bytes > 0.0 {
                        target_in_memory_row_group_size / estimated_row_size_bytes
                    } else {
                        target_in_memory_row_group_size
                    } as usize,
                );

                let row_group_writer_factory = TargetBatchWriterFactory {
                    writer_factory: Arc::new(base_writer_factory),
                    target_in_memory_chunk_rows: target_row_group_rows,
                };

                if let Some(partition_cols) = &file_info.partition_cols {
                    let file_writer_factory = TargetFileSizeWriterFactory {
                        writer_factory: Arc::new(row_group_writer_factory),
                        target_in_memory_file_rows: target_file_rows,
                    };
                    let partitioned_writer = Box::new(PartitionedWriter::new(
                        Arc::new(file_writer_factory),
                        partition_cols.clone(),
                    ));
                    (
                        partitioned_writer as Box<dyn FileWriter<Input = _, Result = _>>,
                        "PartitionedParquetWrite",
                    )
                } else {
                    (
                        Box::new(TargetFileSizeWriter::new(
                            target_file_rows,
                            Arc::new(row_group_writer_factory),
                            None,
                        )?) as Box<dyn FileWriter<Input = _, Result = _>>,
                        "UnpartitionedParquetWrite",
                    )
                }
            }
            FileFormat::Csv => {
                let target_in_memory_file_size =
                    cfg.csv_target_filesize as f64 * cfg.csv_inflation_factor;
                let target_file_rows = if estimated_row_size_bytes > 0.0 {
                    target_in_memory_file_size / estimated_row_size_bytes
                } else {
                    target_in_memory_file_size
                } as usize;

                if let Some(partition_cols) = &file_info.partition_cols {
                    let file_writer_factory = TargetFileSizeWriterFactory {
                        writer_factory: Arc::new(base_writer_factory),
                        target_in_memory_file_rows: target_file_rows,
                    };
                    let partitioned_writer = Box::new(PartitionedWriter::new(
                        Arc::new(file_writer_factory),
                        partition_cols.clone(),
                    ));
                    (
                        partitioned_writer as Box<dyn FileWriter<Input = _, Result = _>>,
                        "PartitionedCsvWrite",
                    )
                } else {
                    (
                        Box::new(TargetFileSizeWriter::new(
                            target_file_rows,
                            Arc::new(base_writer_factory),
                            None,
                        )?) as Box<dyn FileWriter<Input = _, Result = _>>,
                        "UnpartitionedCsvWrite",
                    )
                }
            }
            _ => unreachable!("Physical write should only support Parquet and CSV"),
        };
        Ok(Self {
            name,
            writer,
            file_schema,
        })
    }
    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for WriteSink {
    #[instrument(skip_all, name = "WriteSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.writer.write(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        let result = self.writer.close()?;
        let mp = Arc::new(MicroPartition::new_loaded(
            self.file_schema.clone(),
            result.into(),
            None,
        ));
        Ok(Some(mp.into()))
    }
    fn name(&self) -> &'static str {
        self.name
    }
}
