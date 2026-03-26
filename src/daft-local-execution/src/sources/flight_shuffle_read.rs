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
use daft_shuffles::client::FlightClientManager;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::NodeName,
};

pub struct FlightShuffleReadSource {
    receiver: Option<UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>>,
    shuffle_id: u64,
    server_cache_mapping: HashMap<String, Vec<u32>>,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl FlightShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        shuffle_id: u64,
        server_cache_mapping: HashMap<String, Vec<u32>>,
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
            receiver: Some(receiver),
            shuffle_id,
            server_cache_mapping,
            schema,
            num_parallel_tasks,
        }
    }

    fn spawn_flight_shuffle_processor(
        &self,
        mut receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        output_sender: Sender<Arc<MicroPartition>>,
        schema: SchemaRef,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        let num_parallel_tasks = self.num_parallel_tasks;
        let shuffle_id = self.shuffle_id;
        let server_cache_mapping = self.server_cache_mapping.clone();
        io_runtime.spawn(async move {
            let mut client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<FlightShuffleReadInput> = VecDeque::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && !pending_tasks.is_empty() {
                    let input = pending_tasks
                        .pop_front()
                        .expect("Pending tasks should not be empty");
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
                    ));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((_input_id, inputs)) if inputs.is_empty() => {
                                let empty = Arc::new(MicroPartition::empty(Some(schema.clone())));
                                if output_sender.send(empty).await.is_err() {
                                    return Ok(());
                                }
                            }
                            Some((_input_id, inputs)) => {
                                for input in inputs {
                                    pending_tasks.push_back(input);
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

            Ok(())
        })
    }
}

async fn forward_partition_stream(
    mut stream: BoxStream<'static, DaftResult<daft_recordbatch::RecordBatch>>,
    schema: SchemaRef,
    sender: Sender<Arc<MicroPartition>>,
) -> DaftResult<()> {
    while let Some(batch) = stream.next().await {
        let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
        if sender.send(Arc::new(mp)).await.is_err() {
            break;
        }
    }
    Ok(())
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
        let (output_sender, output_receiver) = create_channel::<Arc<MicroPartition>>(1);
        let mut this = *self;
        let input_receiver = this.receiver.take().expect("Receiver not found");

        let processor_task =
            this.spawn_flight_shuffle_processor(input_receiver, output_sender, this.schema.clone());

        let result_stream = output_receiver
            .into_stream()
            .map(|mp| Ok(Arc::try_unwrap(mp).unwrap_or_else(|arc| (*arc).clone())));
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}
