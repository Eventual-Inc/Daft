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
use daft_local_plan::{FlightShufflePartitionRef, FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::client::FlightClientManager;
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
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl ShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
        shuffle_id: u64,
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
            shuffle_id,
            schema,
            num_parallel_tasks,
        }
    }

    fn spawn_flight_shuffle_processor(
        self,
        output_sender: Sender<PipelineMessage>,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let mut receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, FlightShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks && let Some((input_id, input)) = pending_tasks.pop_front() {
                    task_set.spawn(forward_partition_refs(
                        client_manager.clone(),
                        input.refs,
                        self.schema.clone(),
                        output_sender.clone(),
                        input_id,
                    ));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((input_id, inputs)) if inputs.is_empty() => {
                                let empty = MicroPartition::empty(Some(self.schema.clone()));
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

async fn fetch_partition_refs(
    client_manager: FlightClientManager,
    refs: &[FlightShufflePartitionRef],
    schema: SchemaRef,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let refs = refs
        .iter()
        .filter(|partition_ref| partition_ref.num_rows > 0 && partition_ref.size_bytes > 0)
        .collect::<Vec<_>>();

    if refs.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    let mut refs_by_server: HashMap<&str, Vec<&FlightShufflePartitionRef>> = HashMap::new();
    for partition_ref in refs {
        refs_by_server
            .entry(partition_ref.server_address.as_str())
            .or_default()
            .push(partition_ref);
    }
    let fetches = refs_by_server
        .into_iter()
        .map(|(server_address, server_refs)| {
            let client_manager = client_manager.clone();
            let schema = schema.clone();
            let partition_ref_ids = server_refs
                .iter()
                .map(|partition_ref| partition_ref.partition_ref_id)
                .collect::<Vec<_>>();
            let shuffle_id = server_refs
                .first()
                .expect("expected at least one partition ref")
                .shuffle_id;
            let server_address = server_address.to_string();
            async move {
                client_manager
                    .fetch_record_batches(shuffle_id, &server_address, &partition_ref_ids, schema)
                    .await
            }
        })
        .collect::<Vec<_>>();
    let streams = futures::future::try_join_all(fetches).await?;

    Ok(futures::stream::select_all(streams).boxed())
}

async fn forward_partition_refs(
    client_manager: FlightClientManager,
    refs: Vec<FlightShufflePartitionRef>,
    schema: SchemaRef,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
) -> DaftResult<InputId> {
    let nonempty_ref_count = refs
        .iter()
        .filter(|partition_ref| partition_ref.num_rows > 0 && partition_ref.size_bytes > 0)
        .count();
    if nonempty_ref_count == 0 {
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition: MicroPartition::empty(Some(schema)),
            })
            .await
            .is_err()
        {
            return Ok(input_id);
        }
        return Ok(input_id);
    }

    let mut batches = fetch_partition_refs(client_manager, &refs, schema.clone()).await?;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        let partition = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
        if sender
            .send(PipelineMessage::Morsel {
                input_id,
                partition,
            })
            .await
            .is_err()
        {
            return Ok(input_id);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::{DataType, Field, Schema};

    use super::*;
    use crate::{channel::create_channel, pipeline::PipelineMessage};

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]))
    }

    #[tokio::test]
    async fn forward_partition_refs_emits_single_empty_morsel_when_all_refs_are_empty()
    -> DaftResult<()> {
        let schema = make_schema();
        let refs = vec![
            FlightShufflePartitionRef {
                shuffle_id: 11,
                server_address: "grpc://worker-a:1234".to_string(),
                partition_ref_id: 101,
                num_rows: 0,
                size_bytes: 100,
            },
            FlightShufflePartitionRef {
                shuffle_id: 11,
                server_address: "grpc://worker-b:1234".to_string(),
                partition_ref_id: 102,
                num_rows: 4,
                size_bytes: 0,
            },
        ];
        let (sender, mut receiver) = create_channel(4);

        let completed_input_id = forward_partition_refs(
            FlightClientManager::new(),
            refs,
            schema.clone(),
            sender.clone(),
            7,
        )
        .await?;
        drop(sender);

        assert_eq!(completed_input_id, 7);

        let Some(PipelineMessage::Morsel {
            input_id,
            partition,
        }) = receiver.recv().await
        else {
            panic!("expected empty morsel");
        };
        assert_eq!(input_id, 7);
        assert_eq!(partition.len(), 0);
        assert_eq!(partition.schema(), schema);
        assert!(receiver.recv().await.is_none());

        Ok(())
    }
}
