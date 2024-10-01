use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_micropartition::{FileWriter, MicroPartition};
use daft_table::Table;
use snafu::ResultExt;

use super::WriteOperator;
use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    create_task_set,
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, TaskSet, NUM_CPUS,
};

pub(crate) struct UnpartitionedWriteNode {
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    write_operator: Arc<dyn WriteOperator>,
    target_in_memory_file_rows: usize,
    target_in_memory_chunk_rows: usize,
    file_schema: SchemaRef,
}

impl UnpartitionedWriteNode {
    pub(crate) fn new(
        child: Box<dyn PipelineNode>,
        write_operator: Arc<dyn WriteOperator>,
        target_in_memory_file_rows: usize,
        target_in_memory_chunk_rows: usize,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            child,
            runtime_stats: RuntimeStatsContext::new(),
            write_operator,
            target_in_memory_file_rows,
            target_in_memory_chunk_rows,
            file_schema,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    async fn run_writer(
        mut input_receiver: Receiver<(Arc<MicroPartition>, usize)>,
        write_operator: Arc<dyn WriteOperator>,
    ) -> DaftResult<Vec<Table>> {
        let mut written_file_paths = vec![];
        let mut current_writer: Option<Box<dyn FileWriter>> = None;
        let mut current_file_idx = None;
        while let Some((data, file_idx)) = input_receiver.recv().await {
            if current_file_idx.is_none() || current_file_idx.unwrap() != file_idx {
                if let Some(writer) = current_writer.take() {
                    if let Some(path) = writer.close()? {
                        written_file_paths.push(path);
                    }
                }
                current_file_idx = Some(file_idx);
                current_writer = Some(write_operator.create_writer(file_idx, None)?);
            }
            if let Some(writer) = current_writer.as_mut() {
                writer.write(&data)?;
            }
        }
        if let Some(writer) = current_writer {
            if let Some(path) = writer.close()? {
                written_file_paths.push(path);
            }
        }
        Ok(written_file_paths)
    }

    fn spawn_writers(
        num_writers: usize,
        task_set: &mut TaskSet<DaftResult<Vec<Table>>>,
        write_operator: &Arc<dyn WriteOperator>,
    ) -> Vec<Sender<(Arc<MicroPartition>, usize)>> {
        let mut writer_senders = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            let (writer_sender, writer_receiver) = create_channel(1);
            task_set.spawn(Self::run_writer(writer_receiver, write_operator.clone()));
            writer_senders.push(writer_sender);
        }
        writer_senders
    }

    async fn dispatch(
        mut input_receiver: CountingReceiver,
        target_chunk_rows: usize,
        target_file_rows: usize,
        senders: Vec<Sender<(Arc<MicroPartition>, usize)>>,
    ) -> DaftResult<()> {
        let mut curr_sent_rows = 0;
        let mut curr_file_idx = 0;
        let mut curr_sender_idx = 0;
        let mut buffer = RowBasedBuffer::new(target_chunk_rows);
        while let Some(data) = input_receiver.recv().await {
            let data = data.as_data();
            if data.is_empty() {
                continue;
            }

            buffer.push(data.clone());
            if let Some(ready) = buffer.pop_enough()? {
                for part in ready {
                    curr_sent_rows += part.len();
                    let _ = senders[curr_sender_idx].send((part, curr_file_idx)).await;
                    if curr_sent_rows >= target_file_rows {
                        curr_sent_rows = 0;
                        curr_file_idx += 1;
                        curr_sender_idx = (curr_sender_idx + 1) % senders.len();
                    }
                }
            }
        }
        if let Some(leftover) = buffer.pop_all()? {
            let _ = senders[curr_file_idx].send((leftover, curr_file_idx)).await;
        }
        Ok(())
    }
}

impl TreeDisplay for UnpartitionedWriteNode {
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

impl PipelineNode for UnpartitionedWriteNode {
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
        // Start children
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        // Initialize destination channel
        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);

        // Start writers
        let write_operator = self.write_operator.clone();
        let mut task_set = create_task_set();
        let writer_senders = Self::spawn_writers(*NUM_CPUS, &mut task_set, &write_operator);

        // Start dispatch
        let (target_file_rows, target_chunk_rows) = (
            self.target_in_memory_file_rows,
            self.target_in_memory_chunk_rows,
        );
        runtime_handle.spawn(
            Self::dispatch(
                child_results_receiver,
                target_chunk_rows,
                target_file_rows,
                writer_senders,
            ),
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
                let mp = MicroPartition::new_loaded(schema, results.into(), None);
                let _ = destination_sender.send(Arc::new(mp).into()).await;
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
