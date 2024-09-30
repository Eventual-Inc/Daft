use std::{collections::HashMap, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_core::{
    prelude::{SchemaRef, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::{FileWriter, MicroPartition};
use daft_table::Table;
use snafu::ResultExt;

use super::unpartitioned_write::WriteOperator;
use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, NUM_CPUS,
};

struct PerPartitionWriter {
    write_operator: Arc<dyn WriteOperator>,
    writer: Box<dyn FileWriter>,
    target_file_rows: usize,
    results: Vec<Table>,
    written_rows_so_far: usize,
    buffer: RowBasedBuffer,
    partition_value: Table,
    partition_postfix: Arc<str>,
}

impl PerPartitionWriter {
    fn new(
        write_operator: Arc<dyn WriteOperator>,
        partition_value: Table,
        partition_postfix: Arc<str>,
        target_file_rows: usize,
        target_chunk_rows: usize,
    ) -> DaftResult<Self> {
        Ok(Self {
            writer: write_operator.create_partitioned_writer(
                0,
                &partition_value,
                &partition_postfix,
            )?,
            write_operator,
            target_file_rows,
            results: vec![],
            written_rows_so_far: 0,
            buffer: RowBasedBuffer::new(target_chunk_rows),
            partition_value,
            partition_postfix,
        })
    }

    fn submit(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        self.buffer.push(data.clone());
        if let Some(ready) = self.buffer.pop_enough()? {
            for part in ready {
                self.write(&part)?;
            }
        }
        Ok(())
    }

    fn write(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        let len = data.len();
        self.writer.write(data)?;
        self.written_rows_so_far += len;

        // Check if the file is full, close and start a new file if necessary
        if self.written_rows_so_far >= self.target_file_rows {
            let result = self.writer.close()?;
            if let Some(result) = result {
                self.results.push(result);
            }
            self.written_rows_so_far = 0;
            self.writer = self.write_operator.create_partitioned_writer(
                self.results.len(),
                &self.partition_value,
                &self.partition_postfix,
            )?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> DaftResult<Vec<Table>> {
        // Write any remaining data from the buffer
        let remaining = self.buffer.pop_all()?;
        if let Some(part) = remaining {
            self.write(&part)?;
        }

        // Finalize the current file and collect results
        if let Some(result) = self.writer.close()? {
            self.results.push(result);
        }
        Ok(std::mem::take(&mut self.results))
    }
}

pub(crate) struct PartitionedWriteNode {
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    write_operator: Arc<dyn WriteOperator>,
    partition_cols: Vec<ExprRef>,
    default_partition_fallback: Arc<str>,
    target_file_rows: usize,
    target_chunk_rows: usize,
    file_schema: SchemaRef,
}

impl PartitionedWriteNode {
    pub(crate) fn new(
        child: Box<dyn PipelineNode>,
        write_operator: Arc<dyn WriteOperator>,
        partition_cols: Vec<ExprRef>,
        default_partition_fallback: Arc<str>,
        target_file_rows: usize,
        target_chunk_rows: usize,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            child,
            runtime_stats: RuntimeStatsContext::new(),
            partition_cols,
            write_operator,
            default_partition_fallback,
            target_file_rows,
            target_chunk_rows,
            file_schema,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    fn partition(
        partition_cols: &[ExprRef],
        default_partition_fallback: &Arc<str>,
        data: &Arc<MicroPartition>,
    ) -> DaftResult<(Vec<MicroPartition>, Table, Vec<Series>)> {
        let (split_tables, partition_values) = data.partition_by_value(partition_cols)?;
        let concated = partition_values
            .concat_or_get(IOStatsContext::new("MicroPartition::partition_by_value"))?;
        let partition_values_table = concated.first().unwrap();
        let pkey_names = partition_values_table.column_names();

        let mut values_string_values = Vec::with_capacity(pkey_names.len());
        for name in pkey_names.iter() {
            let column = partition_values_table.get_column(name)?;
            let string_names = column.to_str_values()?;
            let default_part = Utf8Array::from_iter(
                "default",
                std::iter::once(Some(default_partition_fallback.clone())),
            )
            .into_series();
            let null_filled = string_names.if_else(&default_part, &column.not_null()?)?;
            values_string_values.push(null_filled);
        }

        Ok((
            split_tables,
            partition_values_table.clone(),
            values_string_values,
        ))
    }

    async fn run_writer(
        mut input_receiver: Receiver<Arc<MicroPartition>>,
        write_operator: Arc<dyn WriteOperator>,
        partition_cols: Vec<ExprRef>,
        default_partition_fallback: Arc<str>,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> DaftResult<Vec<Table>> {
        let mut per_partition_writers = HashMap::new();
        while let Some(data) = input_receiver.recv().await {
            let (split_tables, partition_values, partition_values_strs) =
                Self::partition(&partition_cols, &default_partition_fallback, &data)?;
            for (idx, partition) in split_tables.into_iter().enumerate() {
                let postfix = partition_values
                    .column_names()
                    .iter()
                    .zip(partition_values_strs.iter())
                    .map(|(pkey, values)| {
                        format!("{}={}", pkey, values.utf8().unwrap().get(idx).unwrap())
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                let per_partition_writer = if !per_partition_writers.contains_key(&postfix) {
                    let partition_value_row = partition_values.slice(idx, idx + 1)?;
                    per_partition_writers.insert(
                        postfix.clone(),
                        PerPartitionWriter::new(
                            write_operator.clone(),
                            partition_value_row,
                            postfix.clone().into(),
                            target_file_rows,
                            target_chunk_rows,
                        )?,
                    );
                    per_partition_writers.get_mut(&postfix).unwrap()
                } else {
                    per_partition_writers.get_mut(&postfix).unwrap()
                };

                per_partition_writer.submit(&Arc::new(partition))?;
            }
        }
        let mut results = vec![];
        for writer in per_partition_writers.values_mut() {
            let res = writer.finalize()?;
            results.extend(res);
        }
        Ok(results)
    }

    fn spawn_writers(
        num_writers: usize,
        task_set: &mut tokio::task::JoinSet<DaftResult<Vec<Table>>>,
        write_operator: Arc<dyn WriteOperator>,
        partition_cols: Vec<ExprRef>,
        default_partition_fallback: Arc<str>,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> Vec<Sender<Arc<MicroPartition>>> {
        let mut writer_senders = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            let (writer_sender, writer_receiver) = create_channel(1);
            task_set.spawn(Self::run_writer(
                writer_receiver,
                write_operator.clone(),
                partition_cols.clone(),
                default_partition_fallback.clone(),
                target_chunk_rows,
                target_file_rows,
            ));
            writer_senders.push(writer_sender);
        }
        writer_senders
    }

    async fn dispatch(
        mut input_receiver: CountingReceiver,
        senders: Vec<Sender<Arc<MicroPartition>>>,
        partition_cols: Vec<ExprRef>,
    ) -> DaftResult<()> {
        while let Some(data) = input_receiver.recv().await {
            let data = data.as_data();
            let partitioned = data.partition_by_hash(&partition_cols, senders.len())?;
            for (idx, mp) in partitioned.into_iter().enumerate() {
                if !mp.is_empty() {
                    let _ = senders[idx].send(mp.into()).await;
                }
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
        self.write_operator.name()
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);
        let target_chunk_rows = self.target_chunk_rows;
        let target_file_rows = self.target_file_rows;
        let partition_cols = self.partition_cols.clone();
        let schema = self.file_schema.clone();
        let mut task_set = tokio::task::JoinSet::new();
        let writer_senders = Self::spawn_writers(
            *NUM_CPUS,
            &mut task_set,
            self.write_operator.clone(),
            self.partition_cols.clone(),
            self.default_partition_fallback.clone(),
            target_chunk_rows,
            target_file_rows,
        );
        runtime_handle.spawn(
            Self::dispatch(child_results_receiver, writer_senders, partition_cols),
            self.name(),
        );
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
