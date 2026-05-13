use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, combine_stream, get_compute_pool_num_threads, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_local_plan::{FlightShuffleReadInput, FlightShuffleServerGroup, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    client::FlightClientManager, multi_partition_cache::record_read_queue_dispatch,
    server::flight_server::ShuffleFlightServer,
};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream, StatsProvider};
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::{NodeName, PipelineMessage},
};

pub struct ShuffleReadSource {
    receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
    local_server: Arc<ShuffleFlightServer>,
    local_address: String,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        local_server: Arc<ShuffleFlightServer>,
        local_address: String,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };

        Self {
            receiver,
            local_server,
            local_address,
            schema,
            num_parallel_tasks,
        }
    }

    async fn get_partition_stream(
        client_manager: FlightClientManager,
        local_server: Arc<ShuffleFlightServer>,
        local_address: &str,
        server_groups: Vec<FlightShuffleServerGroup>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        // server_groups are pre-grouped by (shuffle_id, server_address) at task-build
        // time, so each group is exactly one Flight RPC (or one local call).
        let mut streams: Vec<BoxStream<'static, DaftResult<RecordBatch>>> = Vec::new();
        let mut remote_fetches = Vec::new();

        for group in server_groups {
            if group.server_address == local_address {
                let local_stream = local_server
                    .get_partition_local(group.shuffle_id, &group.partition_ref_ids)
                    .await?;
                streams.push(local_stream);
            } else {
                let client_manager = client_manager.clone();
                let schema = schema.clone();
                remote_fetches.push(async move {
                    client_manager
                        .fetch_partition(
                            group.shuffle_id,
                            &group.server_address,
                            &group.partition_ref_ids,
                            schema,
                        )
                        .await
                });
            }
        }

        let remote_streams = futures::future::try_join_all(remote_fetches).await?;
        streams.extend(remote_streams);

        if streams.is_empty() {
            Ok(futures::stream::empty().boxed())
        } else {
            Ok(futures::stream::select_all(streams).boxed())
        }
    }

    fn spawn_flight_shuffle_processor(
        self,
        output_sender: Sender<PipelineMessage>,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let mut receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;
        let local_server = self.local_server;
        let local_address = self.local_address.clone();
        let schema = self.schema;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, FlightShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks
                    && let Some((input_id, input)) = pending_tasks.pop_front()
                {
                    // Snapshot queue state before consuming `input` so the
                    // queue-time-coalescing instrumentation sees what a batched
                    // dispatcher could have batched with. Counts how many of the
                    // remaining queued entries target ≥1 source server in common
                    // with the one we're about to dispatch.
                    let queue_depth_before = pending_tasks.len() + 1;
                    let same_server_count = {
                        let dispatched_servers: std::collections::HashSet<&str> = input
                            .server_groups
                            .iter()
                            .map(|g| g.server_address.as_str())
                            .collect();
                        pending_tasks
                            .iter()
                            .filter(|(_, other)| {
                                other
                                    .server_groups
                                    .iter()
                                    .any(|g| dispatched_servers.contains(g.server_address.as_str()))
                            })
                            .count()
                    };
                    record_read_queue_dispatch(queue_depth_before, same_server_count);
                    let stream = Self::get_partition_stream(client_manager.clone(), local_server.clone(), &local_address, input.server_groups, schema.clone()).await?;
                    task_set.spawn(forward_partition_stream(stream, schema.clone(), output_sender.clone(), input_id));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((input_id, inputs)) if inputs.is_empty() => {
                                let empty = MicroPartition::empty(Some(schema.clone()));
                                if output_sender.send(PipelineMessage::Morsel {
                                    input_id,
                                    partition: empty,
                                }).await.is_err() {
                                    return Ok(());
                                }
                                if output_sender.send(PipelineMessage::Flush(input_id)).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Some((input_id, inputs)) => {
                                let num_inputs = inputs.len();
                                *input_id_pending_counts.entry(input_id).or_insert(0) += num_inputs;
                                for input in inputs {
                                    pending_tasks.push_back((input_id, input));
                                }
                            }
                            None => {
                                receiver_exhausted = true;
                            }
                        }
                    }
                    Some(join_result) = task_set.join_next(), if !task_set.is_empty() => {
                        match join_result {
                            Ok(Ok(completed_input_id)) => {
                                let count = input_id_pending_counts.get_mut(&completed_input_id).expect("Input id should be present in input_id_pending_counts");
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    input_id_pending_counts.remove(&completed_input_id);
                                    if output_sender.send(PipelineMessage::Flush(completed_input_id)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

async fn forward_partition_stream(
    mut stream: BoxStream<'static, DaftResult<daft_recordbatch::RecordBatch>>,
    schema: SchemaRef,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
) -> DaftResult<InputId> {
    let mut emitted_any = false;
    while let Some(batch) = stream.next().await {
        let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
        emitted_any = true;
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: mp,
            })
            .await
            .is_err()
        {
            return Ok(input_id);
        }
    }
    // If the stream produced no batches (all refs were zero-row / file-less),
    // still emit a single empty `MicroPartition` so the downstream pipeline
    // sees one output per input. Matches the `inputs.is_empty()` branch in
    // `spawn_flight_shuffle_processor`.
    if !emitted_any {
        let empty = MicroPartition::empty(Some(schema.clone()));
        let _ = sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: empty,
            })
            .await;
    }
    Ok(input_id)
}

#[async_trait]
impl Source for ShuffleReadSource {
    fn name(&self) -> NodeName {
        "ShuffleRead".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["ShuffleRead".to_string()]
    }

    #[instrument(skip_all, name = "ShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        _stats_provider: StatsProvider,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        // The forward_partition_stream tasks (one per Flight fetch, up to
        // num_parallel_tasks concurrent) all send through this channel.
        // With capacity=1 a slow downstream consumer stalls EVERY forward
        // task on `sender.send().await`, which suspends their host stream
        // — `select_all` then stops being polled, all underlying Flight
        // streams sit Pending, and the read_agg `gap_after_pending` bucket
        // balloons.
        //
        // Default capacity 64 batches lets the producer pipeline run
        // ~4 batches ahead per forward task before blocking. Tunable via
        // `DAFT_SHUFFLE_READ_CHANNEL_CAPACITY`. Peak memory ceiling is
        // capacity × avg_batch_size (~4 MiB target) = ~256 MiB.
        let capacity = std::env::var("DAFT_SHUFFLE_READ_CHANNEL_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(64);
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(capacity);
        let processor_task = self.spawn_flight_shuffle_processor(output_sender);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}
