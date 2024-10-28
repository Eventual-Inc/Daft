use std::sync::Arc;

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_micropartition::{FileWriter, MicroPartition};
use daft_table::Table;
use snafu::ResultExt;

use super::WriterFactory;
use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, TaskSet, NUM_CPUS,
};

pub(crate) struct UnpartitionedWriteNode {
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    writer_factory: Arc<dyn WriterFactory>,
    target_in_memory_file_rows: usize,
    target_in_memory_chunk_rows: usize,
    file_schema: SchemaRef,
}

impl UnpartitionedWriteNode {
    pub(crate) fn new(
        name: &'static str,
        child: Box<dyn PipelineNode>,
        writer_factory: Arc<dyn WriterFactory>,
        target_in_memory_file_rows: usize,
        target_in_memory_chunk_rows: usize,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(),
            writer_factory,
            target_in_memory_file_rows,
            target_in_memory_chunk_rows,
            file_schema,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    // Receives data from the dispatcher and writes it.
    // If the received file idx is different from the current file idx, this means that the current file is full and needs to be closed.
    // Once input is exhausted, the current writer is closed and all written file paths are returned.
    async fn run_writer(
        mut input_receiver: Receiver<(Arc<MicroPartition>, usize)>,
        writer_factory: Arc<dyn WriterFactory>,
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
                current_writer = Some(writer_factory.create_writer(file_idx, None)?);
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
        writer_factory: &Arc<dyn WriterFactory>,
        channel_size: usize,
    ) -> Vec<Sender<(Arc<MicroPartition>, usize)>> {
        let mut writer_senders = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            let (writer_sender, writer_receiver) = create_channel(channel_size);
            task_set.spawn(Self::run_writer(writer_receiver, writer_factory.clone()));
            writer_senders.push(writer_sender);
        }
        writer_senders
    }

    // Dispatches data received from the child to the writers
    // As data is received, it is buffered until enough data is available to fill a chunk
    // Once a chunk is filled, it is sent to a writer
    // If the writer has written enough rows for a file, increment the file index and switch to the next writer
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
            let _ = senders[curr_sender_idx]
                .send((leftover, curr_file_idx))
                .await;
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

        // Initialize destination channel
        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);

        // Start writers
        let writer_factory = self.writer_factory.clone();
        let mut task_set = TaskSet::new();
        let writer_senders = Self::spawn_writers(
            *NUM_CPUS,
            &mut task_set,
            &writer_factory,
            // The channel size is set to the number of chunks per file such that writes can be parallelized
            (self.target_in_memory_file_rows + self.target_in_memory_chunk_rows + 1)
                / self.target_in_memory_chunk_rows,
        );

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
