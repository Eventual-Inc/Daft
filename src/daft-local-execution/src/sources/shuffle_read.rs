use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
#[cfg(feature = "celeborn")]
use common_runtime::get_compute_runtime;
use common_runtime::{JoinSet, combine_stream, get_compute_pool_num_threads, get_io_runtime};
use daft_core::prelude::SchemaRef;
#[cfg(feature = "celeborn")]
use daft_io::IOStatsRef;
#[cfg(feature = "celeborn")]
use daft_local_plan::CelebornShuffleReadInput;
use daft_local_plan::{FlightShuffleReadInput, InputId};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
#[cfg(feature = "celeborn")]
use daft_shuffles::client::celeborn::CelebornClient;
use daft_shuffles::{
    client::FlightClientManager, server::flight_server::ShuffleFlightServer,
    shuffle_cache::partition_ref_id,
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

    /// Resolve read inputs to the exact `(shuffle_id, server_address, partition_ref_ids)`
    /// requests to issue, merged so each server is contacted once.
    fn to_server_requests(inputs: Vec<FlightShuffleReadInput>) -> Vec<(u64, String, Vec<u64>)> {
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

    async fn get_partition_stream(
        client_manager: FlightClientManager,
        local_server: Arc<ShuffleFlightServer>,
        local_address: &str,
        inputs: Vec<FlightShuffleReadInput>,
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let (local_requests, remote_requests): (Vec<_>, Vec<_>) = Self::to_server_requests(inputs)
            .into_iter()
            .partition(|(_, address, _)| address == local_address);

        let mut local_streams = Vec::new();
        for (shuffle_id, _, partition_ref_ids) in local_requests {
            local_streams.push(
                local_server
                    .get_partition_local(shuffle_id, &partition_ref_ids)
                    .await?,
            );
        }

        let fetches = remote_requests
            .into_iter()
            .map(|(shuffle_id, server_address, partition_ref_ids)| {
                let client_manager = client_manager.clone();
                let schema = schema.clone();
                async move {
                    client_manager
                        .fetch_partition(shuffle_id, &server_address, &partition_ref_ids, schema)
                        .await
                }
            })
            .collect::<Vec<_>>();
        let remote_streams = futures::future::try_join_all(fetches).await?;

        Ok(futures::stream::select_all(local_streams.into_iter().chain(remote_streams)).boxed())
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
            let mut pending_tasks: VecDeque<(InputId, Vec<FlightShuffleReadInput>)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                while task_set.len() < num_parallel_tasks
                    && let Some((input_id, inputs)) = pending_tasks.pop_front()
                {
                    let stream = Self::get_partition_stream(client_manager.clone(), local_server.clone(), &local_address, inputs, schema.clone()).await?;
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

// =====================================================================
// Celeborn shuffle read source
// =====================================================================

/// Reduce-side source that pulls partition data from a Celeborn cluster.
///
/// In contrast with [`ShuffleReadSource`] (Flight), Celeborn aggregates each
/// reduce partition cluster-side and exposes it as a single logical stream of
/// Arrow IPC bytes. There is no "local vs remote" distinction and no
/// per-mapper fan-out on the reader: one `read_partition` call returns all
/// blocks for the partition.
#[cfg(feature = "celeborn")]
pub struct CelebornShuffleReadSource {
    receiver: UnboundedReceiver<(InputId, Vec<CelebornShuffleReadInput>)>,
    shuffle_id: u64,
    num_mappers: u32,
    client: Arc<dyn CelebornClient>,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

#[cfg(feature = "celeborn")]
impl CelebornShuffleReadSource {
    pub fn try_new(
        receiver: UnboundedReceiver<(InputId, Vec<CelebornShuffleReadInput>)>,
        shuffle_id: u64,
        num_mappers: u32,
        client: Arc<dyn CelebornClient>,
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
            shuffle_id,
            num_mappers,
            client,
            schema,
            num_parallel_tasks,
        })
    }

    fn spawn_celeborn_shuffle_processor(
        self,
        output_sender: Sender<PipelineMessage>,
        stats_provider: StatsProvider,
        morsel_target_rows: usize,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let mut receiver = self.receiver;
        let num_parallel_tasks = self.num_parallel_tasks;
        let shuffle_id = self.shuffle_id;
        let num_mappers = self.num_mappers;
        let schema = self.schema.clone();
        let client = self.client.clone();

        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            // num_partitions=0: the read side only uses num_mappers from
            // shuffle_meta (for open_partition); num_partitions is unused by
            // read_partition and the FFI's open_partition call.
            client.register_shuffle(shuffle_id, num_mappers, 0).await?;

            let mut task_set: JoinSet<DaftResult<InputId>> = JoinSet::new();
            let mut pending_tasks: VecDeque<(InputId, CelebornShuffleReadInput)> = VecDeque::new();
            let mut input_id_pending_counts: HashMap<InputId, usize> = HashMap::new();
            let mut receiver_exhausted = false;

            while !receiver_exhausted || !pending_tasks.is_empty() || !task_set.is_empty() {
                // Drain pending partition fetches up to the parallelism cap.
                while task_set.len() < num_parallel_tasks
                    && let Some((input_id, input)) = pending_tasks.pop_front()
                {
                    let stream = client
                        .read_partition(shuffle_id, input.partition_idx as u32)
                        .await?;
                    task_set.spawn(forward_celeborn_partition_stream(
                        stream,
                        schema.clone(),
                        output_sender.clone(),
                        input_id,
                        morsel_target_rows,
                        stats_provider.get_or_create(input_id).io_stats,
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

            // Clean up local shuffle metadata now that all reduce tasks
            // have completed. Server-side cleanup relies on Celeborn's
            // own GC mechanism; this only removes the local entry from
            // the client's `shuffle_meta` map to prevent unbounded growth.
            if let Err(e) = client.unregister_shuffle(shuffle_id).await {
                tracing::warn!(
                    shuffle_id,
                    error = %e,
                    "Failed to unregister Celeborn shuffle; \
                     local metadata may leak until client is dropped"
                );
            }

            Ok(())
        })
    }
}

/// Fuse a run of small `RecordBatch`es into one.
///
/// Runs on the compute pool: the concat preallocates per column and memcpys, and
/// the caller lives on the IO runtime (see `forward_celeborn_partition_stream`).
///
/// `concat_copied` rather than `concat` because the batches were decoded zero-copy
/// out of the transport buffer: a one-batch run would otherwise be handed back
/// untouched and keep the whole decoded partition alive for as long as the write
/// takes.
#[cfg(feature = "celeborn")]
async fn fuse_batches(
    compute_runtime: &common_runtime::RuntimeRef,
    batches: Vec<RecordBatch>,
) -> DaftResult<RecordBatch> {
    compute_runtime
        .spawn(async move { RecordBatch::concat_copied(&batches) })
        .await?
}

/// Wrap a fused batch in a `MicroPartition` carrying the source's declared
/// schema — the same one the empty-partition path uses, so every Morsel this
/// source emits reports one schema.
///
/// The caller must have checked the decoded batches against `schema` already
/// (see `check_decoded_schema`): `new_loaded` asserts every batch's schema
/// equals the `MicroPartition`'s.
#[cfg(feature = "celeborn")]
fn morsel_from_batch(schema: SchemaRef, batch: RecordBatch) -> MicroPartition {
    MicroPartition::new_loaded(schema, Arc::new(vec![batch]), None)
}

/// Reject a reduce partition whose decoded schema is not the one the plan says
/// this source produces.
///
/// The map side writes the post-repartition schema, so this should never fire —
/// but `new_loaded` would otherwise turn a mismatch into a panic on an IO
/// runtime thread, and taking the batch's own schema instead would hand
/// downstream operators columns their bound expressions do not match. Both are
/// worse than a named error.
#[cfg(feature = "celeborn")]
fn check_decoded_schema(
    expected: &SchemaRef,
    batches: &[RecordBatch],
    input_id: InputId,
) -> DaftResult<()> {
    // Every batch in one decoded buffer already shares a schema
    // (`read_record_batches_from_ipc_streams` rejects a buffer that mixes them),
    // so one check per buffer covers it.
    let Some(first) = batches.first() else {
        return Ok(());
    };
    if first.schema != *expected {
        return Err(common_error::DaftError::SchemaMismatch(format!(
            "Celeborn shuffle read: partition for input {input_id} decoded with a schema the \
             plan does not expect: {} vs {}",
            first.schema, expected
        )));
    }
    Ok(())
}

/// Fuse a run and hand it downstream. `Ok(false)` if the receiver is gone.
#[cfg(feature = "celeborn")]
async fn send_fused_morsel(
    compute_runtime: &common_runtime::RuntimeRef,
    sender: &Sender<PipelineMessage>,
    input_id: InputId,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> DaftResult<bool> {
    let fused = fuse_batches(compute_runtime, batches).await?;
    Ok(sender
        .send(PipelineMessage::Morsel {
            input_id,
            partition: morsel_from_batch(schema, fused),
        })
        .await
        .is_ok())
}

/// Decode the partition bytes returned by `CelebornClient::read_partition`
/// (a concatenation of per-push Arrow IPC streams) exactly once, fuse the decoded
/// batches into runs of at most `morsel_target_rows`, and forward each as a `Morsel`.
#[cfg(feature = "celeborn")]
async fn forward_celeborn_partition_stream(
    mut stream: BoxStream<'static, DaftResult<bytes::Bytes>>,
    schema: SchemaRef,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
    morsel_target_rows: usize,
    io_stats: IOStatsRef,
) -> DaftResult<InputId> {
    // A reduce partition arrives as many small self-contained Arrow IPC streams —
    // one per mapper push (map-side accumulator drain). At high `repartition(N)`
    // each push is tiny (a partition's `total_bytes / N` sliver), so forwarding a
    // push as it arrives floods the pipeline with hundreds of ~KB items: each pays a
    // bounded-channel hop, a write call, and a trip through the consumer (for a
    // Python one, a GIL-held Arrow export per column). That fixed per-item overhead,
    // not the read or the write itself, dominates the reduce stage.
    //
    // So accumulate decoded batches up to `morsel_target_rows` and fuse each run into
    // the batch a Morsel carries. That target is the `chunk_size` the pipeline asked
    // this source for, so these Morsels are sized like every other source's; there is
    // nothing between a Morsel and a decoded batch that wants a size of its own.
    //
    // The target applies per decoded batch, not per stream chunk: `read_partition`
    // hands back the whole partition in a single buffer (`ffi.rs`, `read_to_end`), so
    // this is the only thing bounding Morsel size. Sizing has to happen here because
    // the Celeborn client hands back an undifferentiated byte stream and knows
    // nothing about Morsels.
    //
    // Rows rather than bytes: `size_bytes()` costs a walk over batches x columns and
    // reports 0 for an all-Null column, which would let a run grow unbounded. `len()`
    // is exact and free.
    //
    // A run is cut *before* the batch that would cross the target, so a Morsel stays
    // under it. A decoded batch is never split to land on the target exactly: the
    // target is a ceiling, and a Morsel that comes in short costs nothing. The one
    // thing that can exceed it is a single push already larger than the target, which
    // has to go out whole.

    // Both the IPC decode and the fuse are CPU-bound, but this task runs on the
    // IO runtime (`spawn_celeborn_shuffle_processor`), whose threads should stay
    // parked on the Celeborn fetches of the other partitions in flight. Hand the
    // CPU work to the compute pool — which is sized for it — and keep only the
    // stream polling and the channel sends here.
    let compute_runtime = get_compute_runtime();

    let mut sent_any = false;
    // Decoded batches accumulating toward one Morsel, and their row count.
    let mut buffered: Vec<RecordBatch> = Vec::new();
    let mut buffered_rows: usize = 0;

    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        if bytes.is_empty() {
            continue;
        }
        let num_bytes = bytes.len();
        // `bytes` holds the partition's per-push Arrow IPC streams concatenated
        // (see `RepartitionSink::sink` Celeborn branch). Decode them all in a
        // single pass, off the IO runtime.
        //
        // The decode borrows from `bytes` rather than copying, so the batches keep
        // this whole buffer alive until the fuse below copies them out. Hand the
        // bytes over by value and keep no other reference here.
        let batches = compute_runtime
            .spawn(async move { MicroPartition::read_record_batches_from_ipc_streams(bytes) })
            .await?
            .map_err(|e| {
                common_error::DaftError::External(
                    format!(
                        "Celeborn shuffle read: failed to decode Arrow IPC streams \
                         ({num_bytes} bytes) for input {input_id}: {e}"
                    )
                    .into(),
                )
            })?;
        check_decoded_schema(&schema, &batches, input_id)?;
        // The bytes this reduce task pulled off the cluster. Counted once the
        // buffer has decoded, so a partition that fails to decode is not
        // reported as read.
        io_stats.mark_bytes_read(num_bytes);

        for batch in batches {
            // Cut the run before this batch if it would cross the target. `buffered`
            // is only non-empty here while it is still short of the target, so a
            // batch bigger than the target on its own still goes out (alone, on the
            // next pass or in the tail) rather than blocking progress.
            if !buffered.is_empty() && buffered_rows + batch.len() > morsel_target_rows {
                buffered_rows = 0;
                // Fuse once per run, at its boundary — never incrementally as batches
                // arrive, which would re-copy the whole run on every one.
                if !send_fused_morsel(
                    &compute_runtime,
                    &sender,
                    input_id,
                    schema.clone(),
                    std::mem::take(&mut buffered),
                )
                .await?
                {
                    return Ok(input_id);
                }
                sent_any = true;
            }
            buffered_rows += batch.len();
            buffered.push(batch);
        }
    }

    // Flush the tail (the common case at high `repartition(N)`: the whole
    // partition is smaller than the target and never crossed it mid-stream).
    if !buffered.is_empty()
        && send_fused_morsel(
            &compute_runtime,
            &sender,
            input_id,
            schema.clone(),
            buffered,
        )
        .await?
    {
        sent_any = true;
    }
    // Daft's pipeline contract requires every `input_id` to emit at least one
    // Morsel before its terminating `Flush`: `IntermediateNode` treats a `Flush`
    // for an input it has no buffered data for as "this node is done" and tears
    // the whole (shared, multi-input) pipeline down (see `intermediate_op.rs`,
    // the `!has_input(input_id)` branch). An empty reduce partition — the common
    // case under `repartition(N)` with N ≫ rows — otherwise sends only a Flush,
    // so we emit an empty Morsel first to keep `has_input` true, mirroring the
    // `inputs.is_empty()` branch in the processor.
    if !sent_any {
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

#[cfg(feature = "celeborn")]
#[async_trait]
impl Source for CelebornShuffleReadSource {
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
        vec![format!(
            "ShuffleRead(Celeborn): shuffle_id={}",
            self.shuffle_id
        )]
    }

    #[instrument(skip_all, name = "CelebornShuffleReadSource::get_data")]
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        stats_provider: StatsProvider,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        // `chunk_size` is the row count the pipeline wants per Morsel from any
        // source: a downstream `Strict` morsel size requirement if there is one,
        // otherwise `default_morsel_size`.
        let processor_task =
            self.spawn_celeborn_shuffle_processor(output_sender, stats_provider, chunk_size);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
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

#[cfg(all(test, feature = "celeborn"))]
mod tests {
    use daft_core::{datatypes::Int64Array, series::IntoSeries};

    use super::*;

    const INPUT_ID: InputId = 7;

    fn batch_of(values: Vec<i64>) -> DaftResult<RecordBatch> {
        RecordBatch::from_nonempty_columns(vec![Int64Array::from_vec("a", values).into_series()])
    }

    /// One map-side push: a self-contained Arrow IPC stream. The reduce read
    /// hands back these concatenated (see `ffi.rs`).
    fn push_of(batch: &RecordBatch) -> DaftResult<Vec<u8>> {
        MicroPartition::new_loaded(batch.schema.clone(), Arc::new(vec![batch.clone()]), None)
            .write_to_ipc_stream()
    }

    /// The bytes one reduce partition is read as: its pushes concatenated.
    fn partition_bytes(pushes: &[RecordBatch]) -> DaftResult<bytes::Bytes> {
        let mut bytes = Vec::new();
        for batch in pushes {
            bytes.extend_from_slice(&push_of(batch)?);
        }
        Ok(bytes.into())
    }

    /// Run the forwarder over one partition's worth of bytes and collect what it
    /// emitted, alongside the bytes it reported reading. The channel is
    /// deliberately small, so draining concurrently is what keeps a multi-morsel
    /// partition from deadlocking on backpressure.
    async fn forward_with_stats(
        bytes: bytes::Bytes,
        schema: SchemaRef,
        target_rows: usize,
    ) -> DaftResult<(Vec<MicroPartition>, usize)> {
        let (sender, mut receiver) = create_channel::<PipelineMessage>(1);
        let stream: BoxStream<'static, DaftResult<bytes::Bytes>> = if bytes.is_empty() {
            futures::stream::empty().boxed()
        } else {
            futures::stream::once(async move { Ok(bytes) }).boxed()
        };
        let io_stats = IOStatsRef::default();
        let task = tokio::spawn(forward_celeborn_partition_stream(
            stream,
            schema,
            sender,
            INPUT_ID,
            target_rows,
            io_stats.clone(),
        ));

        let mut morsels = Vec::new();
        while let Some(message) = receiver.recv().await {
            match message {
                PipelineMessage::Morsel {
                    input_id,
                    partition,
                } => {
                    assert_eq!(input_id, INPUT_ID);
                    morsels.push(partition);
                }
                other => panic!("expected only Morsels, got {other:?}"),
            }
        }
        assert_eq!(task.await.expect("forwarder panicked")?, INPUT_ID);
        Ok((morsels, io_stats.load_bytes_read()))
    }

    async fn forward(
        bytes: bytes::Bytes,
        schema: SchemaRef,
        target_rows: usize,
    ) -> DaftResult<Vec<MicroPartition>> {
        Ok(forward_with_stats(bytes, schema, target_rows).await?.0)
    }

    /// The whole point of fusing is that rows survive it. Check the payload, not
    /// just the count: a concat that dropped or reordered a run would still add
    /// up if only lengths were compared.
    #[tokio::test]
    async fn celeborn_forward_preserves_rows_in_order() -> DaftResult<()> {
        let pushes = vec![
            batch_of(vec![1, 2])?,
            batch_of(vec![3])?,
            batch_of(vec![4, 5, 6])?,
        ];
        let schema = pushes[0].schema.clone();
        let bytes = partition_bytes(&pushes)?;

        let morsels = forward(bytes, schema, 1024).await?;
        let [morsel] = morsels.as_slice() else {
            panic!("expected one morsel for a partition well under the target");
        };
        assert_eq!(
            RecordBatch::concat(morsel.record_batches())?,
            RecordBatch::concat(&pushes)?
        );
        // Fusing happened. Not "exactly one batch": that a run fuses into one is
        // what the target arithmetic happens to yield, not a shape anything relies
        // on — a `MicroPartition` holds several batches by design.
        assert!(
            morsel.record_batches().len() < pushes.len(),
            "expected the pushes to be fused, got {} batches for {} pushes",
            morsel.record_batches().len(),
            pushes.len()
        );
        Ok(())
    }

    /// Past the morsel target the partition has to come out as several morsels —
    /// still whole, still in order, and none of them over the target. Pushes of
    /// 2/3/3 rows against a target of 4: the second push would make 5, so the run
    /// is cut before it rather than after, and the third leaves 3 for the tail.
    #[tokio::test]
    async fn celeborn_forward_splits_before_crossing_the_target() -> DaftResult<()> {
        const TARGET_ROWS: usize = 4;
        let pushes = vec![
            batch_of(vec![1, 2])?,
            batch_of(vec![3, 4, 5])?,
            batch_of(vec![6, 7, 8])?,
        ];
        let schema = pushes[0].schema.clone();
        let bytes = partition_bytes(&pushes)?;

        let morsels = forward(bytes, schema, TARGET_ROWS).await?;
        assert_eq!(
            morsels.iter().map(MicroPartition::len).collect::<Vec<_>>(),
            vec![2, 3, 3]
        );
        let batches = morsels
            .iter()
            .flat_map(|morsel| morsel.record_batches())
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            RecordBatch::concat(&batches)?,
            RecordBatch::concat(&pushes)?
        );
        Ok(())
    }

    /// A push already over the target is the one thing that can exceed it: cutting
    /// it down would mean splitting a decoded batch, which the fusing deliberately
    /// does not do. It goes out whole, on its own.
    #[tokio::test]
    async fn celeborn_forward_emits_an_oversized_push_whole() -> DaftResult<()> {
        let pushes = vec![batch_of(vec![1, 2, 3, 4, 5])?, batch_of(vec![6])?];
        let schema = pushes[0].schema.clone();
        let bytes = partition_bytes(&pushes)?;

        let morsels = forward(bytes, schema, 2).await?;
        assert_eq!(
            morsels.iter().map(MicroPartition::len).collect::<Vec<_>>(),
            vec![5, 1]
        );
        Ok(())
    }

    /// An empty reduce partition must still emit one Morsel: `IntermediateNode`
    /// reads a Flush for an input it has no data for as "this node is done" and
    /// tears down the whole shared pipeline.
    #[tokio::test]
    async fn celeborn_forward_empty_partition_still_emits_a_morsel() -> DaftResult<()> {
        let schema = batch_of(vec![1])?.schema.clone();
        let morsels = forward(bytes::Bytes::new(), schema.clone(), 1024).await?;

        let [morsel] = morsels.as_slice() else {
            panic!("expected exactly one morsel, got {}", morsels.len());
        };
        assert_eq!(morsel.len(), 0);
        assert_eq!(morsel.schema(), schema);
        Ok(())
    }

    /// The reduce stage reads every byte of its partitions off the cluster, so
    /// it has to report them: with nothing counted here the query's IO stats
    /// claim a shuffle read moved no data at all.
    #[tokio::test]
    async fn celeborn_forward_reports_the_bytes_it_read() -> DaftResult<()> {
        let pushes = vec![batch_of(vec![1, 2])?, batch_of(vec![3])?];
        let schema = pushes[0].schema.clone();
        let bytes = partition_bytes(&pushes)?;
        let num_bytes = bytes.len();

        let (morsels, bytes_read) = forward_with_stats(bytes, schema, 1024).await?;
        assert_eq!(morsels.iter().map(MicroPartition::len).sum::<usize>(), 3);
        assert_eq!(bytes_read, num_bytes);
        Ok(())
    }

    /// A partition that decodes to a schema the plan does not expect is an
    /// error, not something to forward: `new_loaded` would assert on it deep
    /// inside an IO runtime thread, and taking the decoded schema instead would
    /// hand downstream operators columns their bound expressions do not match.
    #[tokio::test]
    async fn celeborn_forward_rejects_an_unexpected_schema() -> DaftResult<()> {
        let bytes = partition_bytes(&[batch_of(vec![1, 2])?])?;
        // Same shape, different column name — what a stale plan or a crossed
        // shuffle id would look like on the wire.
        let other_schema = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("b", vec![1]).into_series(),
        ])?
        .schema;

        let err = forward(bytes, other_schema, 1024)
            .await
            .expect_err("expected a schema mismatch");
        assert!(
            matches!(err, common_error::DaftError::SchemaMismatch(_)),
            "expected a SchemaMismatch, got {err:?}"
        );
        Ok(())
    }
}
