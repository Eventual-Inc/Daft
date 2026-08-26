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
use daft_local_plan::{FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    client::FlightClientManager,
    server::flight_server::ShuffleFlightServer,
    shuffle_cache::partition_ref_id,
    store::{ShuffleReadSource as ReadRoute, reader::read_partition_stream},
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
    /// This worker's routing preference. Deliberately read from the local config
    /// rather than baked into the plan: which route is faster depends on the
    /// reader's own link to its peers and to the shared mount.
    read_route: ReadRoute,
    shared_read_concurrency: usize,
}

impl ShuffleReadSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<FlightShuffleReadInput>)>,
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

        Ok(Self {
            receiver,
            local_server,
            local_address,
            schema,
            num_parallel_tasks,
            read_route: ReadRoute::parse(Some(&cfg.flight_shuffle_read_source))?,
            shared_read_concurrency: cfg.flight_shuffle_shared_read_concurrency,
        })
    }

    /// Resolve read inputs to the exact `(shuffle_id, server_address, partition_ref_ids)`
    /// requests to issue, merged so each server is contacted once.
    fn to_server_requests(inputs: &[FlightShuffleReadInput]) -> Vec<(u64, String, Vec<u64>)> {
        let mut refs_by_server: HashMap<(u64, String), Vec<u64>> = HashMap::new();
        for input in inputs {
            for (address, input_ids) in input.inputs_by_server.iter() {
                refs_by_server
                    .entry((input.shuffle_id, address.clone()))
                    .or_default()
                    .extend(
                        input_ids
                            .iter()
                            .map(|id| partition_ref_id(*id, input.partition_idx as usize)),
                    );
            }
        }
        refs_by_server
            .into_iter()
            .map(|((shuffle_id, address), ref_ids)| (shuffle_id, address, ref_ids))
            .collect()
    }

    /// Shared-mount root per shuffle, when that shuffle used shared placement.
    fn shared_roots(inputs: &[FlightShuffleReadInput]) -> HashMap<u64, Arc<str>> {
        inputs
            .iter()
            .filter_map(|input| {
                input
                    .shared_root
                    .as_ref()
                    .map(|root| (input.shuffle_id, root.clone()))
            })
            .collect()
    }

    async fn get_partition_stream(
        client_manager: FlightClientManager,
        local_server: Arc<ShuffleFlightServer>,
        local_address: &str,
        inputs: Vec<FlightShuffleReadInput>,
        schema: SchemaRef,
        read_route: ReadRoute,
        shared_read_concurrency: usize,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let shared_roots = Self::shared_roots(&inputs);
        let requests = Self::to_server_requests(&inputs);

        let mut streams: Vec<BoxStream<'static, DaftResult<RecordBatch>>> =
            Vec::with_capacity(requests.len());
        for (shuffle_id, address, partition_ref_ids) in requests {
            let shared_root = shared_roots.get(&shuffle_id).cloned();

            // This worker wrote it: serve in-process and skip both the network and
            // the on-disk index, since the byte ranges are already in memory.
            if address == local_address {
                streams.push(
                    local_server
                        .get_partition_local(shuffle_id, &partition_ref_ids)
                        .await?,
                );
                continue;
            }

            let use_shared = match (read_route, shared_root.is_some()) {
                (ReadRoute::Shared, false) => {
                    return Err(DaftError::ValueError(format!(
                        "flight_shuffle_read_source='shared' but shuffle {} was not written to a \
                         shared directory; set flight_shuffle_placement='shared_only'",
                        shuffle_id
                    )));
                }
                (ReadRoute::Rpc, _) => false,
                // Reading the mount directly beats proxying through the writer: it
                // is the same storage either way, minus a hop and the writer's CPU.
                (ReadRoute::Auto, has_shared) | (ReadRoute::Shared, has_shared) => has_shared,
            };

            if use_shared {
                let root = shared_root.expect("checked above");
                streams.push(shared_stream_for_refs(
                    &root,
                    shuffle_id,
                    &partition_ref_ids,
                    schema.clone(),
                    shared_read_concurrency,
                )?);
            } else {
                streams.push(rpc_stream_with_shared_fallback(
                    client_manager.clone(),
                    shuffle_id,
                    address,
                    partition_ref_ids,
                    schema.clone(),
                    shared_root,
                    shared_read_concurrency,
                ));
            }
        }

        Ok(futures::stream::select_all(streams).boxed())
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
        let read_route = self.read_route;
        let shared_read_concurrency = self.shared_read_concurrency;

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let client_manager = FlightClientManager::new();
            let mut task_set = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, Vec<FlightShuffleReadInput>)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks
                    && let Some((input_id, inputs)) = pending_tasks.pop_front()
                {
                    let stream = Self::get_partition_stream(client_manager.clone(), local_server.clone(), &local_address, inputs, schema.clone(), read_route, shared_read_concurrency).await?;
                    task_set.spawn(forward_partition_stream(stream, schema.clone(), output_sender.clone(), input_id));
                }

                tokio::select! {
                    recv_result = receiver.recv(), if !receiver_exhausted => {
                        match recv_result {
                            Some((input_id, inputs)) => {
                                *input_id_pending_counts.entry(input_id).or_insert(0) += 1;
                                pending_tasks.push_back((input_id, inputs));
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

/// Group `partition_ref_ids` back into the `(partition_idx, input_ids)` shape the
/// shared reader addresses files by.
///
/// `partition_ref_id` packs both halves as `(input_id << 32) | partition_idx`, so
/// nothing is lost by having merged them into one request earlier.
fn refs_by_partition_idx(partition_ref_ids: &[u64]) -> HashMap<u32, Vec<u32>> {
    let mut by_partition: HashMap<u32, Vec<u32>> = HashMap::new();
    for ref_id in partition_ref_ids {
        let input_id = (ref_id >> 32) as u32;
        let partition_idx = (ref_id & 0xFFFF_FFFF) as u32;
        by_partition
            .entry(partition_idx)
            .or_default()
            .push(input_id);
    }
    by_partition
}

/// Read a set of refs straight off the shared mount.
fn shared_stream_for_refs(
    shared_root: &str,
    shuffle_id: u64,
    partition_ref_ids: &[u64],
    schema: SchemaRef,
    concurrency: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let mut streams = Vec::new();
    for (partition_idx, input_ids) in refs_by_partition_idx(partition_ref_ids) {
        streams.push(read_partition_stream(
            shared_root,
            shuffle_id,
            &input_ids,
            partition_idx,
            schema.clone(),
            concurrency,
        )?);
    }
    Ok(futures::stream::select_all(streams).boxed())
}

/// Fetch one server's contribution over gRPC, falling back to the shared mount if
/// the RPC fails before yielding anything.
///
/// A worker disappearing looks exactly like this — the connection is refused, or
/// the server hangs up on the first message — and being able to finish the read
/// from shared storage instead is the point of writing it there. The fallback is
/// deliberately limited to failures that happen before the first batch: once
/// batches have gone downstream, re-reading the same refs would deliver them
/// twice, so a mid-stream error has to propagate.
fn rpc_stream_with_shared_fallback(
    client_manager: FlightClientManager,
    shuffle_id: u64,
    server_address: String,
    partition_ref_ids: Vec<u64>,
    schema: SchemaRef,
    shared_root: Option<Arc<str>>,
    shared_read_concurrency: usize,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    Box::pin(async_stream::try_stream! {
        let mut emitted_any = false;
        let mut failure: Option<DaftError> = None;

        match client_manager
            .fetch_partition(shuffle_id, &server_address, &partition_ref_ids, schema.clone())
            .await
        {
            Ok(mut stream) => {
                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(batch) => {
                            emitted_any = true;
                            yield batch;
                        }
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    }
                }
            }
            Err(e) => failure = Some(e),
        }

        if let Some(e) = failure {
            let Some(shared_root) = shared_root.filter(|_| !emitted_any) else {
                Err(e)?;
                return;
            };
            tracing::warn!(
                "Shuffle read from {} failed ({}); falling back to shared storage at {}",
                server_address,
                e,
                shared_root,
            );
            let mut fallback = shared_stream_for_refs(
                &shared_root,
                shuffle_id,
                &partition_ref_ids,
                schema,
                shared_read_concurrency,
            )?;
            while let Some(batch) = fallback.next().await {
                yield batch?;
            }
        }
    })
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
    // If the stream produced no batches (no read inputs, or all refs were
    // zero-row / file-less), still emit a single empty `MicroPartition` so the
    // downstream pipeline sees one output per input.
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
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        let processor_task = self.spawn_flight_shuffle_processor(output_sender);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, Field, Schema};
    use daft_micropartition::MicroPartition;
    use daft_shuffles::{
        oneshot_writer::{OneShotTarget, write_partitions_one_shot},
        store::ShuffleDurability,
    };
    use daft_writers::test::make_dummy_mp;
    use futures::TryStreamExt;

    use super::*;

    fn dummy_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ints", DataType::UInt8)]))
    }

    #[test]
    fn refs_regroup_into_partition_and_input_ids() {
        // (input_id << 32) | partition_idx
        let refs = vec![(7u64 << 32) | 1, (8u64 << 32) | 1, (7u64 << 32) | 2];
        let grouped = refs_by_partition_idx(&refs);
        let mut p1 = grouped[&1].clone();
        p1.sort_unstable();
        assert_eq!(p1, vec![7, 8]);
        assert_eq!(grouped[&2], vec![7]);
    }

    /// A worker that has gone away shows up as an RPC that fails before yielding
    /// anything; the read should complete from shared storage instead of failing
    /// the query.
    #[tokio::test]
    async fn rpc_failure_falls_back_to_shared_storage() -> DaftResult<()> {
        let dir = std::env::temp_dir().join(format!("daft_fallback_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let root = dir.to_str().unwrap().to_string();
        let schema = dummy_schema();
        let (shuffle_id, input_id) = (11u64, 3u32);

        write_partitions_one_shot(
            input_id,
            shuffle_id,
            OneShotTarget::Shared {
                shared_root: root.clone(),
                durability: ShuffleDurability::None,
            },
            schema.clone(),
            None,
            vec![
                make_dummy_mp(64),
                MicroPartition::empty(Some(schema.clone())),
            ],
        )
        .await?;

        // Port 1 is never listening, so the fetch fails at connect time.
        let stream = rpc_stream_with_shared_fallback(
            FlightClientManager::new(),
            shuffle_id,
            "grpc://127.0.0.1:1".to_string(),
            vec![partition_ref_id(input_id, 0)],
            schema,
            Some(Arc::from(root.as_str())),
            4,
        );
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let rows: usize = batches.iter().map(daft_recordbatch::RecordBatch::len).sum();
        assert_eq!(rows, 64, "fallback should have produced the shared copy");

        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// Without a shared copy there is nothing to fall back to, so the RPC error
    /// must surface rather than being swallowed into an empty result.
    #[tokio::test]
    async fn rpc_failure_without_shared_copy_propagates() {
        let stream = rpc_stream_with_shared_fallback(
            FlightClientManager::new(),
            12,
            "grpc://127.0.0.1:1".to_string(),
            vec![partition_ref_id(0, 0)],
            dummy_schema(),
            None,
            4,
        );
        let result: DaftResult<Vec<RecordBatch>> = stream.try_collect().await;
        assert!(result.is_err(), "expected the RPC failure to propagate");
    }
}
