use std::{
    collections::{hash_map::RawEntryMut, HashMap},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::{array::ops::as_arrow::AsArrow, utils::identity_hash_set::IndexHash};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

/// PartitionedWriter is a writer that partitions the input data by a set of columns, and writes each partition
/// to a separate file. It uses a map to keep track of the writers for each partition.
struct PartitionedWriter {
    // TODO: Figure out a way to NOT use the IndexHash + RawEntryMut pattern here. Ideally we want to store ScalarValues, aka. single Rows of the partition values as keys for the hashmap.
    per_partition_writers:
        HashMap<IndexHash, Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<Table>>>>,
    saved_partition_values: Vec<Table>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    partition_by: Vec<ExprRef>,
    is_closed: bool,
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
            is_closed: false,
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
        assert!(
            !self.is_closed,
            "Cannot write to a closed PartitionedWriter"
        );

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
        self.is_closed = true;
        Ok(results)
    }
}

pub(crate) struct PartitionedWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    partition_cols: Vec<ExprRef>,
}

impl PartitionedWriterFactory {
    pub(crate) fn new(
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
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
    type Result = Vec<Table>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(PartitionedWriter::new(
            self.writer_factory.clone(),
            self.partition_cols.clone(),
        ))
            as Box<
                dyn FileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}
