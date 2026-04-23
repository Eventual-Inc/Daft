use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, combine_stream, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::{InputId, NodeName, PipelineMessage},
    sources::source::{SourceStream, StatsProvider},
};

pub struct InMemorySource {
    receiver: UnboundedReceiver<(InputId, Vec<MicroPartitionRef>)>,
    schema: SchemaRef,
    size_bytes: usize,
}

impl InMemorySource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<MicroPartitionRef>)>,
        schema: SchemaRef,
        size_bytes: usize,
    ) -> Self {
        Self {
            receiver,
            schema,
            size_bytes,
        }
    }

    fn spawn_partition_set_processor(
        mut receiver: UnboundedReceiver<(InputId, Vec<MicroPartitionRef>)>,
        output_sender: Sender<PipelineMessage>,
        schema: SchemaRef,
        stats_provider: StatsProvider,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);

        io_runtime.spawn(async move {
            let mut task_set: JoinSet<DaftResult<()>> = JoinSet::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !task_set.is_empty() {
                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((input_id, partitions)) => {
                                let io_stats = stats_provider.get_or_create(input_id).io_stats;
                                task_set.spawn(forward_partition_batch(
                                    partitions,
                                    schema.clone(),
                                    output_sender.clone(),
                                    input_id,
                                    io_stats,
                                ));
                            }
                            None => {
                                receiver_exhausted = true;
                            }
                        }
                    }
                    Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                        match join_result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                return Err(e);
                            }
                            Err(e) => {
                                return Err(e.into());
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

async fn forward_partition_batch(
    partitions: Vec<MicroPartitionRef>,
    schema: SchemaRef,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
    io_stats: IOStatsRef,
) -> DaftResult<()> {
    if partitions.is_empty() {
        let empty = MicroPartition::empty(Some(schema));
        let _ = sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: empty,
            })
            .await;
    } else {
        for partition in partitions {
            let owned = Arc::try_unwrap(partition).unwrap_or_else(|a| (*a).clone());
            io_stats.mark_bytes_read(owned.size_bytes());
            if sender
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition: owned,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    }
    let _ = sender.send(PipelineMessage::Flush(input_id)).await;
    Ok(())
}

#[async_trait]
impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        stats_provider: StatsProvider,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let input_receiver = self.receiver;

        let processor_task = Self::spawn_partition_set_processor(
            input_receiver,
            output_sender,
            self.schema.clone(),
            stats_provider,
        );

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        "In Memory Scan".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InMemoryScan
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("In Memory Scan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.size_bytes));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
