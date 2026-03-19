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
    pipeline::NodeName,
    pipeline_message::{InputId, PipelineMessage},
    sources::source::SourceStream,
};

pub struct InMemorySource {
    receiver: Option<UnboundedReceiver<(InputId, Vec<MicroPartitionRef>)>>,
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
            receiver: Some(receiver),
            schema,
            size_bytes,
        }
    }

    fn spawn_partition_set_processor(
        &self,
        mut receiver: UnboundedReceiver<(InputId, Vec<MicroPartitionRef>)>,
        output_sender: Sender<PipelineMessage>,
        schema: SchemaRef,
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
                                task_set.spawn(forward_partition_batch(
                                    partitions,
                                    schema.clone(),
                                    output_sender.clone(),
                                    input_id,
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
) -> DaftResult<()> {
    if partitions.is_empty() {
        let empty = Arc::new(MicroPartition::empty(Some(schema)));
        let _ = sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: empty,
            })
            .await;
    } else {
        for partition in partitions {
            if sender
                .send(PipelineMessage::Morsel {
                    input_id,
                    partition,
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
        &mut self,
        _maintain_order: bool,
        io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        io_stats.mark_bytes_read(self.size_bytes);
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let input_receiver = self.receiver.take().expect("Receiver not found");

        let processor_task =
            self.spawn_partition_set_processor(input_receiver, output_sender, self.schema.clone());

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
