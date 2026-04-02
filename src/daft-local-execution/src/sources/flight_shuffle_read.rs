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
use daft_shuffles::{client::FlightClientManager, server::flight_server::ShuffleFlightServer};
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::{NodeName, PipelineMessage},
};

pub struct FlightShuffleReadSource {
    receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
    shuffle_id: u64,
    server_cache_mapping: HashMap<String, Vec<u32>>,
    schema: SchemaRef,
    num_parallel_tasks: usize,
    _local_server: Arc<ShuffleFlightServer>,
}

impl FlightShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        shuffle_id: u64,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
        local_server: Arc<ShuffleFlightServer>,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };
        Self {
            receiver,
            shuffle_id,
            server_cache_mapping,
            schema,
            num_parallel_tasks,
            _local_server: local_server,
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
        let server_cache_mapping = self.server_cache_mapping;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let mut client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, FlightShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && let Some((input_id, input)) = pending_tasks.pop_front() {
                    let stream = client_manager
                        .fetch_partition(
                            shuffle_id,
                            input.partition_idx,
                            &server_cache_mapping,
                            schema.clone(),
                        )
                        .await?;
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
impl Source for FlightShuffleReadSource {
    fn name(&self) -> NodeName {
        "FlightShuffleRead".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("FlightShuffleRead: shuffle_id={}", self.shuffle_id)]
    }

    #[instrument(skip_all, name = "FlightShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let processor_task = self.spawn_flight_shuffle_processor(output_sender);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}
