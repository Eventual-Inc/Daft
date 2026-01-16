use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_runtime::JoinSet;
use daft_core::utils::identity_hash_set::{IdentityBuildHasher, IndexHash};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use hashbrown::{HashMap, hash_map::RawEntryMut};

use crate::{AsyncFileWriter, WriteResult, WriterFactory};

/// PartitionedWriter is a writer that partitions the input data by a set of columns, and writes each partition
/// to a separate file. It uses a map to keep track of the writers for each partition.
struct PartitionedWriter {
    // TODO: Figure out a way to NOT use the IndexHash + RawEntryMut pattern here. Ideally we want to store ScalarValues, aka. single Rows of the partition values as keys for the hashmap.
    #[allow(clippy::type_complexity)]
    per_partition_writers: HashMap<
        IndexHash,
        Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
        IdentityBuildHasher,
    >,
    saved_partition_values: Vec<RecordBatch>,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_by: Vec<BoundExpr>,
    is_closed: bool,
}

impl PartitionedWriter {
    pub fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_by: Vec<BoundExpr>,
    ) -> Self {
        Self {
            per_partition_writers: HashMap::with_capacity_and_hasher(
                64,
                IdentityBuildHasher::default(),
            ),
            saved_partition_values: vec![],
            writer_factory,
            partition_by,
            is_closed: false,
        }
    }

    fn partition(
        partition_cols: &[BoundExpr],
        data: Arc<MicroPartition>,
    ) -> DaftResult<(Vec<RecordBatch>, RecordBatch)> {
        let table = data.concat_or_get()?.unwrap();
        let (split_tables, partition_values) = table.partition_by_value(partition_cols)?;
        Ok((split_tables, partition_values))
    }
}

#[async_trait]
impl AsyncFileWriter for PartitionedWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, input: Arc<MicroPartition>) -> DaftResult<WriteResult> {
        assert!(
            !self.is_closed,
            "Cannot write to a closed PartitionedWriter"
        );
        if input.is_empty() {
            return Ok(WriteResult {
                bytes_written: 0,
                rows_written: 0,
            });
        }

        let (split_tables, partition_values) =
            Self::partition(self.partition_by.as_slice(), input)?;
        let partition_values_hash = partition_values.hash_rows()?;

        // Spawn tasks on compute runtime for writing each table
        let mut joinset: JoinSet<
            DaftResult<(
                IndexHash,
                Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
                WriteResult,
                RecordBatch,
            )>,
        > = JoinSet::new();

        for (idx, (table, partition_value_hash)) in split_tables
            .into_iter()
            .zip(partition_values_hash.values().iter())
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

            let table_data = Arc::new(MicroPartition::new_loaded(
                table.schema.clone(),
                vec![table].into(),
                None,
            ));

            match entry {
                RawEntryMut::Vacant(_entry) => {
                    let mut writer = self
                        .writer_factory
                        .create_writer(0, Some(partition_value_row.as_ref()))?;
                    let index_hash = IndexHash {
                        idx: self.saved_partition_values.len() as u64,
                        hash: *partition_value_hash,
                    };
                    self.saved_partition_values
                        .push(partition_value_row.clone());

                    // Spawn task on compute runtime for writing
                    joinset.spawn(async move {
                        let write_result = writer.write(table_data).await?;
                        Ok((index_hash, writer, write_result, partition_value_row))
                    });
                }
                RawEntryMut::Occupied(entry) => {
                    let (index_hash, mut writer) = entry.remove_entry();

                    // Spawn task on compute runtime for writing
                    joinset.spawn(async move {
                        let write_result = writer.write(table_data).await?;
                        Ok((index_hash, writer, write_result, partition_value_row))
                    });
                }
            }
        }

        // Await all write tasks, put writers back, and aggregate results
        let mut bytes_written = 0;
        let mut rows_written = 0;
        while let Some(result) = joinset.join_next().await {
            let (index_hash, writer, write_result, partition_value_row) = result??;
            bytes_written += write_result.bytes_written;
            rows_written += write_result.rows_written;
            match self
                .per_partition_writers
                .raw_entry_mut()
                .from_hash(index_hash.hash, |other| {
                    (index_hash.hash == other.hash) && {
                        let other_table =
                            self.saved_partition_values.get(other.idx as usize).unwrap();
                        other_table == &partition_value_row
                    }
                }) {
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(index_hash.hash, index_hash, writer);
                }
                RawEntryMut::Occupied(_) => {
                    unreachable!("Should not have occupied entry here")
                }
            }
        }

        Ok(WriteResult {
            bytes_written,
            rows_written,
        })
    }

    fn bytes_written(&self) -> usize {
        self.per_partition_writers
            .values()
            .map(|writer| writer.bytes_written())
            .sum::<usize>()
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        self.per_partition_writers
            .values()
            .flat_map(|writer| writer.bytes_per_file())
            .collect()
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        let mut results = vec![];
        for (_, mut writer) in self.per_partition_writers.drain() {
            results.extend(writer.close().await?);
        }
        self.is_closed = true;
        Ok(results)
    }
}

pub(crate) struct PartitionedWriterFactory {
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_cols: Vec<BoundExpr>,
}

impl PartitionedWriterFactory {
    pub(crate) fn new(
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_cols: Vec<BoundExpr>,
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
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(PartitionedWriter::new(
            self.writer_factory.clone(),
            self.partition_cols.clone(),
        ))
            as Box<
                dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}
