use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, combine_stream, get_compute_pool_num_threads, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_local_plan::{FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{client::FlightClientManager, server::flight_server::ShuffleFlightServer};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::{NodeName, PipelineMessage},
};

pub struct ShuffleReadSource {
    receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
    shuffle_id: u64,
    local_cache_ids: Option<Vec<u32>>,
    remote_cache_mapping: HashMap<String, Vec<u32>>,
    local_server: Arc<ShuffleFlightServer>,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ShuffleReadSource {
    pub fn try_new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        shuffle_id: u64,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        local_server: Arc<ShuffleFlightServer>,
        local_address: String,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> DaftResult<Self> {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };

        let (local_cache_ids, remote_cache_mapping) =
            if server_cache_mapping.contains_key(&local_address) {
                (
                    server_cache_mapping.get(&local_address).cloned(),
                    server_cache_mapping
                        .into_iter()
                        .filter(|(addr, _)| addr.as_str() != local_address)
                        .collect(),
                )
            } else {
                (None, server_cache_mapping)
            };

        if local_cache_ids.is_none() && remote_cache_mapping.is_empty() {
            return Err(DaftError::ValueError(
                "No local or remote flight shuffle partition streams found".to_string(),
            ));
        }

        Ok(Self {
            receiver,
            shuffle_id,
            local_cache_ids,
            remote_cache_mapping,
            local_server,
            schema,
            num_parallel_tasks,
        })
    }

    async fn get_partition_stream(
        client_manager: &mut FlightClientManager,
        local_server: &ShuffleFlightServer,
        local_cache_ids: Option<&[u32]>,
        remote_cache_mapping: &HashMap<String, Vec<u32>>,
        shuffle_id: u64,
        partition_idx: usize,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let local_stream = if let Some(local_cache_ids) = local_cache_ids {
            Some(
                local_server
                    .get_partition_local(shuffle_id, partition_idx, local_cache_ids)
                    .await?,
            )
        } else {
            None
        };

        let remote_stream = if remote_cache_mapping.is_empty() {
            None
        } else {
            Some(
                client_manager
                    .fetch_partition(
                        shuffle_id,
                        partition_idx,
                        remote_cache_mapping,
                        schema.clone(),
                    )
                    .await?,
            )
        };

        match (local_stream, remote_stream) {
            (None, None) => Ok(futures::stream::empty().boxed()),
            (Some(local_stream), None) => Ok(local_stream.boxed()),
            (None, Some(remote_stream)) => Ok(remote_stream.boxed()),
            (Some(local_stream), Some(remote_stream)) => {
                Ok(futures::stream::select(local_stream, remote_stream).boxed())
            }
        }
    }

    fn spawn_flight_shuffle_processor(
        self,
        output_sender: Sender<PipelineMessage>,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let mut receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;
        let shuffle_id = self.shuffle_id;
        let schema = self.schema.clone();

        let local_cache_ids = self.local_cache_ids;
        let remote_cache_mapping = self.remote_cache_mapping;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let mut client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, FlightShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && let Some((input_id, input)) = pending_tasks.pop_front() {
                    let stream = Self::get_partition_stream(
                        &mut client_manager,
                        &self.local_server,
                        local_cache_ids.as_deref(),
                        &remote_cache_mapping,
                        shuffle_id,
                        input.partition_idx,
                        schema.clone(),
                    ).await?;
                    task_set.spawn(forward_partition_stream(
                        stream,
                        schema.clone(),
                        output_sender.clone(),
                        input_id,
                    ));
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
    while let Some(batch) = stream.next().await {
        let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: mp,
            })
            .await
            .is_err()
        {
            break;
        }
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
        vec![format!("ShuffleRead: shuffle_id={}", self.shuffle_id)]
    }

    #[instrument(skip_all, name = "ShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let processor_task = self.spawn_flight_shuffle_processor(output_sender);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        // Count bytes per morsel so they land on `SourceSnapshot.bytes_read`
        // (and therefore roll up as `bytes.read` on the Repartition distributed
        // node via `node_origin_id`). Attributes spill reads to the consumer's
        // live runtime-stats rather than the producer's dead reporter.
        let metered_stream = combined_stream.map(move |msg| {
            if let Ok(PipelineMessage::Morsel { partition, .. }) = &msg {
                io_stats.mark_bytes_read(partition.size_bytes());
            }
            msg
        });

        Ok(Box::pin(metered_stream))
    }
}
