use std::{
    collections::{hash_map::RawEntryMut, HashMap},
    sync::Arc,
};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_core::{
    prelude::{AsArrow as _, SchemaRef},
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::{FileWriter, MicroPartition};
use daft_table::Table;
use itertools::Itertools;
use snafu::ResultExt;

use super::WriterFactory;
use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    create_task_set,
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, NUM_CPUS,
};

struct PerPartitionWriteHandler {
    current_writer: Box<dyn FileWriter>,
    writer_factory: Arc<dyn WriterFactory>,
    partition_values: Table,
    buffer: RowBasedBuffer,
    target_file_rows: usize,
    written_rows_so_far: usize,
    results: Vec<Table>,
}

impl PerPartitionWriteHandler {
    fn new(
        writer_factory: Arc<dyn WriterFactory>,
        partition_values: Table,
        target_file_rows: usize,
        target_chunk_rows: usize,
    ) -> DaftResult<Self> {
        Ok(Self {
            current_writer: writer_factory.create_writer(0, Some(&partition_values))?,
            writer_factory,
            partition_values,
            buffer: RowBasedBuffer::new(target_chunk_rows),
            target_file_rows,
            written_rows_so_far: 0,
            results: vec![],
        })
    }

    fn submit(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        self.buffer.push(data.clone());
        if let Some(ready) = self.buffer.pop_enough()? {
            for part in ready {
                self.handle_write(&part)?;
            }
        }
        Ok(())
    }

    fn handle_write(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        let len = data.len();
        self.current_writer.write(data)?;
        self.written_rows_so_far += len;

        // Check if the file is full, close and start a new file if necessary
        if self.written_rows_so_far >= self.target_file_rows {
            let result = self.current_writer.close()?;
            if let Some(result) = result {
                self.results.push(result);
            }
            self.written_rows_so_far = 0;
            self.current_writer = self
                .writer_factory
                .create_writer(self.results.len(), Some(&self.partition_values))?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> DaftResult<Vec<Table>> {
        // Write any remaining data from the buffer
        let remaining = self.buffer.pop_all()?;
        if let Some(part) = remaining {
            self.handle_write(&part)?;
        }

        // Finalize the current file and collect results
        if let Some(result) = self.current_writer.close()? {
            self.results.push(result);
        }
        Ok(std::mem::take(&mut self.results))
    }
}

pub(crate) struct PartitionedWriteNode {
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    writer_factory: Arc<dyn WriterFactory>,
    partition_cols: Vec<ExprRef>,
    target_file_rows: usize,
    target_chunk_rows: usize,
    file_schema: SchemaRef,
}

impl PartitionedWriteNode {
    pub(crate) fn new(
        name: &'static str,
        child: Box<dyn PipelineNode>,
        writer_factory: Arc<dyn WriterFactory>,
        partition_cols: Vec<ExprRef>,
        target_file_rows: usize,
        target_chunk_rows: usize,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(),
            partition_cols,
            writer_factory,
            target_file_rows,
            target_chunk_rows,
            file_schema,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // Receives data from the dispatcher and writes it.
    // Each partition value is mapped to a PerPartitionWriteHandler, which handles the write.
    // Each worker is responsible for a subset of partition values.
    async fn run_worker(
        mut input_receiver: Receiver<(Table, Table)>,
        writer_factory: Arc<dyn WriterFactory>,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> DaftResult<Vec<Table>> {
        let mut per_partition_writers = HashMap::<
            IndexHash,
            PerPartitionWriteHandler,
            IdentityBuildHasher,
        >::with_capacity_and_hasher(20, Default::default());
        let mut saved_partition_values = vec![];
        while let Some((data, partition_values)) = input_receiver.recv().await {
            assert!(partition_values.len() == 1);
            let hash = partition_values.hash_rows()?.get(0).unwrap();
            let entry = per_partition_writers
                .raw_entry_mut()
                .from_hash(hash, |other| {
                    (hash == other.hash) && {
                        let other_table = saved_partition_values.get(other.idx as usize).unwrap();
                        other_table == &partition_values
                    }
                });
            match entry {
                RawEntryMut::Vacant(entry) => {
                    let mut new_partition_write_handler = PerPartitionWriteHandler::new(
                        writer_factory.clone(),
                        partition_values.clone(),
                        target_file_rows,
                        target_chunk_rows,
                    )?;
                    new_partition_write_handler.submit(&Arc::new(MicroPartition::new_loaded(
                        data.schema.clone(),
                        vec![data].into(),
                        None,
                    )))?;
                    entry.insert_hashed_nocheck(
                        hash,
                        IndexHash {
                            idx: saved_partition_values.len() as u64,
                            hash,
                        },
                        new_partition_write_handler,
                    );
                    saved_partition_values.push(partition_values);
                }
                RawEntryMut::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .submit(&Arc::new(MicroPartition::new_loaded(
                            data.schema.clone(),
                            vec![data].into(),
                            None,
                        )))?;
                }
            }
        }

        let results = per_partition_writers
            .values_mut()
            .map(|writer| writer.finalize())
            .flatten_ok()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(results)
    }

    fn spawn_writers(
        num_writers: usize,
        task_set: &mut tokio::task::JoinSet<DaftResult<Vec<Table>>>,
        writer_factory: Arc<dyn WriterFactory>,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> Vec<Sender<(Table, Table)>> {
        let mut writer_senders = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            let (writer_sender, writer_receiver) = create_channel(1);
            task_set.spawn(Self::run_worker(
                writer_receiver,
                writer_factory.clone(),
                target_chunk_rows,
                target_file_rows,
            ));
            writer_senders.push(writer_sender);
        }
        writer_senders
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

    // Dispatches data received from the child to the writers
    // Partition the data by the partition columns, hash the partition values, and send to the appropriate writer based on the hash
    async fn dispatch(
        mut input_receiver: CountingReceiver,
        senders: Vec<Sender<(Table, Table)>>,
        partition_cols: Vec<ExprRef>,
    ) -> DaftResult<()> {
        while let Some(data) = input_receiver.recv().await {
            let data = data.as_data();
            let (split_tables, partition_values) = Self::partition(&partition_cols, data)?;
            let hashes = partition_values.hash_rows()?;
            for (idx, (partition, hash)) in split_tables
                .into_iter()
                .zip(hashes.as_arrow().values_iter())
                .enumerate()
            {
                let send_to = *hash as usize % senders.len();
                let partition_value_row = partition_values.slice(idx, idx + 1)?;
                let _ = senders[send_to]
                    .send((partition, partition_value_row))
                    .await;
            }
        }
        Ok(())
    }
}

impl TreeDisplay for PartitionedWriteNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::*;
        match level {
            Compact => {}
            _ => {
                let rt_result = self.runtime_stats.result();
                rt_result.display(&mut display, true, true, true).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for PartitionedWriteNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        // Start children
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        // Initialize destination channels
        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);

        // Start writers
        let mut task_set = create_task_set();
        let writer_senders = Self::spawn_writers(
            *NUM_CPUS,
            &mut task_set,
            self.writer_factory.clone(),
            self.target_chunk_rows,
            self.target_file_rows,
        );

        // Start dispatch
        let partition_cols = self.partition_cols.clone();
        runtime_handle.spawn(
            Self::dispatch(child_results_receiver, writer_senders, partition_cols),
            self.name(),
        );

        // Join writers, receive results, and send to destination
        let schema = self.file_schema.clone();
        runtime_handle.spawn(
            async move {
                let mut results = vec![];
                while let Some(result) = task_set.join_next().await {
                    results.extend(result.context(JoinSnafu)??);
                }
                let result_mp =
                    Arc::new(MicroPartition::new_loaded(schema, Arc::new(results), None));
                let _ = destination_sender.send(result_mp.into()).await;
                Ok(())
            },
            self.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
