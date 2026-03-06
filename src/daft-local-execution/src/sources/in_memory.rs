use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_partitioning::PartitionRef;
use common_runtime::{JoinSet, combine_stream, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_local_plan::InputId;
use daft_micropartition::MicroPartition;
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{
    channel::{Receiver, Sender, UnboundedReceiver, create_channel, create_unbounded_channel},
    pipeline::NodeName,
    sources::source::SourceStream,
};

pub struct InMemorySource {
    receiver: Option<UnboundedReceiver<(InputId, Vec<PartitionRef>)>>,
    schema: SchemaRef,
    size_bytes: usize,
}

impl InMemorySource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<PartitionRef>)>,
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
        mut receiver: UnboundedReceiver<(InputId, Vec<PartitionRef>)>,
        output_sender: Sender<Arc<MicroPartition>>,
        schema: SchemaRef,
        maintain_order: bool,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);

        // When maintain_order is true, each partition registers a per-partition
        // channel with the flattener *in order*; the flattener drains them
        // sequentially so the output is ordered even though fetches are concurrent.
        let flattener_state = if maintain_order {
            let (agg_tx, agg_rx) = create_unbounded_channel::<Receiver<Arc<MicroPartition>>>();
            let handle = io_runtime.spawn(run_order_preserving_flattener(
                agg_rx,
                output_sender.clone(),
            ));
            Some((agg_tx, handle))
        } else {
            None
        };

        io_runtime.spawn(async move {
            let mut task_set: JoinSet<DaftResult<()>> = JoinSet::new();
            let mut receiver_exhausted = false;
            let semaphore = Arc::new(tokio::sync::Semaphore::new(16));

            while !receiver_exhausted || !task_set.is_empty() {
                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((_input_id, partitions)) => {
                                if partitions.is_empty() {
                                    let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
                                    if let Some((agg_tx, _)) = &flattener_state {
                                        let (tx, rx) = create_channel(1);
                                        let _ = agg_tx.send(rx);
                                        let _ = tx.send(empty).await;
                                    } else {
                                        let _ = output_sender.send(empty).await;
                                    }
                                } else {
                                    for partition in partitions {
                                        let sender = match &flattener_state {
                                            Some((agg_tx, _)) => {
                                                let (tx, rx) = create_channel(1);
                                                let _ = agg_tx.send(rx);
                                                tx
                                            }
                                            None => output_sender.clone(),
                                        };
                                        let sem = semaphore.clone();
                                        task_set.spawn(async move {
                                            let _permit = sem.acquire().await.unwrap();
                                            let native = partition.fetch_native().await?;
                                            let mp = native.downcast::<MicroPartition>().map_err(|_| {
                                                common_error::DaftError::ComputeError("Failed to downcast partition".to_string())
                                            })?;
                                            let _ = sender.send(mp).await;
                                            Ok(())
                                        });
                                    }
                                }
                            }
                            None => {
                                receiver_exhausted = true;
                            }
                        }
                    }
                    Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                        match join_result {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }

            if let Some((agg_tx, handle)) = flattener_state {
                drop(agg_tx);
                handle.await?;
            }
            Ok(())
        })
    }
}

/// Drains each registered receiver sequentially, preserving the registration
/// order so that output partitions appear in their original input order.
async fn run_order_preserving_flattener(
    mut agg_rx: UnboundedReceiver<Receiver<Arc<MicroPartition>>>,
    output_sender: Sender<Arc<MicroPartition>>,
) {
    while let Some(mut inner_rx) = agg_rx.recv().await {
        while let Some(mp) = inner_rx.recv().await {
            if output_sender.send(mp).await.is_err() {
                return;
            }
        }
    }
}

#[async_trait]
impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    fn get_data(
        &mut self,
        maintain_order: bool,
        io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        io_stats.mark_bytes_read(self.size_bytes);
        let (output_sender, output_receiver) = create_channel::<Arc<MicroPartition>>(1);
        let input_receiver = self.receiver.take().expect("Receiver not found");

        let processor_task = self.spawn_partition_set_processor(
            input_receiver,
            output_sender,
            self.schema.clone(),
            maintain_order,
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
