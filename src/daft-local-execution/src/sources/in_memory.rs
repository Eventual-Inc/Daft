use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
// InputId now comes from pipeline_message module
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{
    channel::{Receiver, Sender, create_channel},
    pipeline::NodeName,
    pipeline_message::{InputId, PipelineMessage},
    sources::source::SourceStream,
};

pub struct InMemorySource {
    receiver: Option<Receiver<(InputId, Vec<MicroPartitionRef>)>>,
    schema: SchemaRef,
    size_bytes: usize,
}

impl InMemorySource {
    pub fn new(
        receiver: Receiver<(InputId, Vec<MicroPartitionRef>)>,
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
        mut receiver: Receiver<(InputId, Vec<MicroPartitionRef>)>,
        output_sender: Sender<PipelineMessage>,
        schema: SchemaRef,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);

        io_runtime.spawn(async move {
            while let Some((input_id, partitions)) = receiver.recv().await {
                if partitions.is_empty() {
                    let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
                    if output_sender
                        .send(PipelineMessage::Morsel {
                            input_id,
                            partition: empty,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                } else {
                    for partition in partitions {
                        if output_sender
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
                // Send flush signal after processing each input batch
                if output_sender
                    .send(PipelineMessage::Flush(input_id))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        })
    }
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
