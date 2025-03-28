use std::{
    collections::{hash_map::RawEntryMut, HashMap, VecDeque},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::{array::ops::as_arrow::AsArrow, utils::identity_hash_set::IndexHash};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{FileWriter, WriterFactory};

struct PartitionWriterManager {
    writers: HashMap<
        IndexHash,
        Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    >,
    lru_queue: VecDeque<IndexHash>,
    max_open_writers: usize,
    results: Vec<RecordBatch>,
    bytes_per_file: Vec<usize>,
    bytes_written: usize,
}

impl PartitionWriterManager {
    fn new(max_open_writers: usize) -> Self {
        Self {
            writers: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_open_writers,
            results: vec![],
            bytes_per_file: vec![],
            bytes_written: 0,
        }
    }

    fn write(
        &mut self,
        key: IndexHash,
        partition_value_row: RecordBatch,
        writer_factory: &Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        data: Arc<MicroPartition>,
    ) -> DaftResult<usize> {
        self.update_lru(&key);

        let (bytes_written, need_to_evict) = match self.writers.raw_entry_mut().from_key(&key) {
            RawEntryMut::Occupied(entry) => {
                let writer = entry.into_mut();
                (writer.write(data)?, false)
            }
            RawEntryMut::Vacant(entry) => {
                let writer = writer_factory.create_writer(0, Some(&partition_value_row))?;
                let entry = entry.insert(key, writer);
                (entry.1.write(data)?, true)
            }
        };

        if need_to_evict {
            self.evict_if_needed()?;
        }

        self.bytes_written += bytes_written;
        Ok(bytes_written)
    }

    fn update_lru(&mut self, key: &IndexHash) {
        if let Some(pos) = self.lru_queue.iter().position(|x| *x == *key) {
            self.lru_queue.remove(pos);
        }
        self.lru_queue.push_back(key.clone());
    }

    fn evict_if_needed(&mut self) -> DaftResult<()> {
        if self.writers.len() > self.max_open_writers {
            println!("Evicting writer");
            if let Some(oldest_key) = self.lru_queue.pop_front() {
                if let Some(mut writer) = self.writers.remove(&oldest_key) {
                    self.results.extend(writer.close()?);
                    self.bytes_per_file.extend(writer.bytes_per_file());
                }
            }
        }
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Vec<RecordBatch>> {
        while let Some(key) = self.lru_queue.pop_front() {
            if let Some(mut writer) = self.writers.remove(&key) {
                self.results.extend(writer.close()?);
                self.bytes_per_file.extend(writer.bytes_per_file());
            }
        }
        Ok(self.results.clone())
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.bytes_per_file.clone()
    }
}

/// PartitionedWriter is a writer that partitions the input data by a set of columns, and writes each partition
/// to a separate file. It uses a map to keep track of the writers for each partition.
struct PartitionedWriter {
    writer_manager: PartitionWriterManager,
    saved_partition_values: Vec<RecordBatch>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_by: Vec<ExprRef>,
    is_closed: bool,
}

impl PartitionedWriter {
    pub fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_by: Vec<ExprRef>,
        max_open_writers: usize,
    ) -> Self {
        Self {
            writer_manager: PartitionWriterManager::new(max_open_writers),
            saved_partition_values: vec![],
            writer_factory,
            partition_by,
            is_closed: false,
        }
    }

    fn partition(
        partition_cols: &[ExprRef],
        data: Arc<MicroPartition>,
    ) -> DaftResult<(Vec<RecordBatch>, RecordBatch)> {
        let data = data.concat_or_get(IOStatsContext::new("MicroPartition::partition_by_value"))?;
        let table = data.first().unwrap();
        let (split_tables, partition_values) = table.partition_by_value(partition_cols)?;
        Ok((split_tables, partition_values))
    }
}

impl FileWriter for PartitionedWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<usize> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed PartitionedWriter"
        );

        let (split_tables, partition_values) =
            Self::partition(self.partition_by.as_slice(), input)?;
        let partition_values_hash = partition_values.hash_rows()?;
        let mut bytes_written = 0;

        for (idx, (table, partition_value_hash)) in split_tables
            .into_iter()
            .zip(partition_values_hash.as_arrow().values_iter())
            .enumerate()
        {
            let partition_value_row = partition_values.slice(idx, idx + 1)?;
            let key = IndexHash {
                idx: self.saved_partition_values.len() as u64,
                hash: *partition_value_hash,
            };

            bytes_written += self.writer_manager.write(
                key,
                partition_value_row.clone(),
                &self.writer_factory,
                Arc::new(MicroPartition::new_loaded(
                    table.schema.clone(),
                    vec![table].into(),
                    None,
                )),
            )?;

            if !self.saved_partition_values.contains(&partition_value_row) {
                self.saved_partition_values.push(partition_value_row);
            }
        }
        Ok(bytes_written)
    }

    fn bytes_written(&self) -> usize {
        self.writer_manager.bytes_written()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.writer_manager.bytes_per_file()
    }

    fn close(&mut self) -> DaftResult<Vec<RecordBatch>> {
        self.is_closed = true;
        self.writer_manager.close()
    }
}

pub(crate) struct PartitionedWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_cols: Vec<ExprRef>,
}

impl PartitionedWriterFactory {
    pub(crate) fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_cols: Vec<ExprRef>,
    ) -> Self {
        Self {
            writer_factory,
            partition_cols,
        }
    }
}
impl WriterFactory for PartitionedWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(PartitionedWriter::new(
            self.writer_factory.clone(),
            self.partition_cols.clone(),
            100, // Default value for max_open_writers
        ))
            as Box<
                dyn FileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}
