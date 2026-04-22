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
use daft_io::IOStatsRef;
use daft_local_plan::{FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{client::FlightClientManager, server::flight_server::ShuffleFlightServer};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{IOStatsProvider, Source, SourceStream};
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
        refs: Vec<FlightPartitionRef>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (local_refs, remote_refs): (Vec<_>, Vec<_>) = refs
            .into_iter()
            .partition(|r| r.server_address == local_address);

        let local_stream = if !local_refs.is_empty() {
            Some(
                local_server
                    .get_partition_local(
                        local_refs[0].shuffle_id,
                        &local_refs
                            .iter()
                            .map(|r| r.partition_ref_id)
                            .collect::<Vec<_>>(),
                    )
                    .await?,
            )
        } else {
            None
        };

        let remote_stream = if remote_refs.is_empty() {
            None
        } else {
            let mut refs_by_server: HashMap<(u64, String), Vec<u64>> = HashMap::new();
            for partition_ref in remote_refs {
                refs_by_server
                    .entry((
                        partition_ref.shuffle_id,
                        partition_ref.server_address.clone(),
                    ))
                    .or_default()
                    .push(partition_ref.partition_ref_id);
            }
            let fetches = refs_by_server
                .into_iter()
                .map(|((shuffle_id, server_address), partition_ref_ids)| {
                    let client_manager = client_manager.clone();
                    let schema = schema.clone();
                    async move {
                        client_manager
                            .fetch_partition(
                                shuffle_id,
                                &server_address,
                                &partition_ref_ids,
                                schema,
                            )
                            .await
                    }
                })
                .collect::<Vec<_>>();
            // Some(futures::future::try_join_all(fetches).await?)
            let streams = futures::future::try_join_all(fetches).await?;
            Some(futures::stream::select_all(streams).boxed())
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
        io_stats_provider: IOStatsProvider,
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
                    let stream = Self::get_partition_stream(client_manager.clone(), local_server.clone(), &local_address, input.refs, schema.clone()).await?;
                    let io_stats = io_stats_provider.get_or_create(input_id);
                    task_set.spawn(forward_partition_stream(stream, schema.clone(), output_sender.clone(), input_id, io_stats));
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
    io_stats: IOStatsRef,
) -> DaftResult<InputId> {
    while let Some(batch) = stream.next().await {
        let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
        io_stats.mark_bytes_read(mp.size_bytes());
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
        vec!["ShuffleRead".to_string()]
    }

    #[instrument(skip_all, name = "ShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        io_stats_provider: IOStatsProvider,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let processor_task =
            self.spawn_flight_shuffle_processor(output_sender, io_stats_provider);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}
