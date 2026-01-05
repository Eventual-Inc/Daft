use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::{partitioning::PartitionSetRef, MicroPartition, MicroPartitionRef};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{
    channel::{Receiver, Sender, create_channel},
    pipeline::NodeName,
    plan_input::{InputId, PipelineMessage},
    sources::source::SourceStream,
};

pub struct StreamingInMemorySource {
    source_id: String,
    receiver: Mutex<Option<Receiver<(InputId, PartitionSetRef<MicroPartitionRef>)>>>,
    schema: SchemaRef,
    size_bytes: usize,
}

impl StreamingInMemorySource {
    pub fn new(
        source_id: String,
        receiver: Receiver<(InputId, PartitionSetRef<MicroPartitionRef>)>,
        schema: SchemaRef,
        size_bytes: usize,
    ) -> Self {
        Self { source_id,
            receiver: Mutex::new(Some(receiver)),
            schema,
            size_bytes,
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }

    /// Spawns the background task that continuously reads partition sets from receiver and processes them
    fn spawn_partition_set_processor(
        &self,
        mut receiver: Receiver<(InputId, PartitionSetRef<MicroPartitionRef>)>,
        output_sender: Sender<PipelineMessage>,
        schema: SchemaRef,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);

        let source_id = self.source_id.clone();
        io_runtime.spawn(async move {
            while let Some((input_id, partition_set)) = receiver.recv().await {
                let mut stream = partition_set.to_partition_stream();
                let mut has_data = false;
                let mut partition_count = 0;
                while let Some(result) = stream.next().await {
                    has_data = true;
                    partition_count += 1;
                    let partition = result?;
                    let message = PipelineMessage::Morsel {
                        input_id,
                        partition: partition.into(),
                    };
                    if output_sender.send(message).await.is_err() {
                        println!("[StreamingInMemorySource] Error sending message");
                        break;
                    }
                }
                
                // If no data for this input_id, send empty micropartition
                if !has_data {
                    let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
                    let message = PipelineMessage::Morsel {
                        input_id,
                        partition: empty,
                    };
                    if output_sender.send(message).await.is_err() {
                        println!("[StreamingInMemorySource] Error sending message");
                        break;
                    }
                }
                
                // Always flush after processing each partition_set
                if output_sender
                    .send(PipelineMessage::Flush(input_id))
                    .await
                    .is_err()
                {
                    println!("[StreamingInMemorySource] Error sending flush message");
                    break;
                }
            }
            println!("[StreamingInMemorySource] Finished sending messages");
            Ok(())
        })
    }
}

#[async_trait]
impl Source for StreamingInMemorySource {
    #[instrument(name = "StreamingInMemorySource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        // Create output channel for results - note: SourceStream still returns Morsel
        // We'll convert PipelineMessage to Morsel in the stream
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        // Spawn a task that continuously reads from self.receiver
        // Receiver implements Clone, so we can clone it for the spawned task
        let receiver_clone = self.receiver.lock().unwrap().take().unwrap();

        // Spawn the partition set processor that continuously reads from receiver
        let processor_task = self.spawn_partition_set_processor(receiver_clone, output_sender, self.schema.clone());

        // Convert receiver to stream, filtering out flush signals and converting Morsels
        let result_stream = output_receiver.into_stream();

        // Combine with processor task to handle errors
        let combined_stream =
            combine_stream(Box::pin(result_stream.map(Ok)), processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        "StreamingInMemorySource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InMemoryScan
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("StreamingInMemorySource:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.size_bytes));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
