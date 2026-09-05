use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::OrderedJoinSet;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_recordbatch::RecordBatch;
#[cfg(feature = "celeborn")]
use daft_shuffles::client::celeborn::CelebornClient;
use daft_shuffles::{oneshot_writer::write_partitions_one_shot, shuffle_cache::CHUNK_TARGET_BYTES};
use itertools::Itertools;
use tracing::{Span, instrument};

#[cfg(feature = "celeborn")]
use super::shuffle_metadata::ShufflePartitionMeta;
use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
    },
    shuffle_backend::LocalShuffleBackend,
};
#[cfg(feature = "celeborn")]
use crate::pipeline::MapperAttemptRegistry;
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

// Worst-case buffered memory is `num_workers × num_inputs × threshold` — one
// accumulator per (worker, input).
const REPARTITION_MIN_BUFFER_THRESHOLD_BYTES: usize = 16 * 1024 * 1024; // 16 MB
const REPARTITION_MAX_BUFFER_THRESHOLD_BYTES: usize = 256 * 1024 * 1024; // 256 MB

/// Per-(worker, input) accumulator. Morsels are buffered until they cross
/// the sink's threshold, then fused and partitioned in one pass.
pub(crate) struct RepartitionAccState {
    post_repartitioned: Vec<Vec<RecordBatch>>,
    pre_repartitioned: Vec<RecordBatch>,
    pre_repartitioned_size_bytes: usize,
    bound_keys: Vec<BoundExpr>,
    repartition_spec: RepartitionSpec,
    input_id: InputId,
}

impl RepartitionAccState {
    fn new(
        num_partitions: usize,
        input_id: InputId,
        bound_keys: Vec<BoundExpr>,
        repartition_spec: RepartitionSpec,
    ) -> Self {
        Self {
            post_repartitioned: (0..num_partitions).map(|_| Vec::new()).collect(),
            pre_repartitioned: Vec::new(),
            pre_repartitioned_size_bytes: 0,
            bound_keys,
            repartition_spec,
            input_id,
        }
    }

    fn num_partitions(&self) -> usize {
        self.post_repartitioned.len()
    }

    /// Fuse pre-repartitioned morsels, partition once, and append to post-repartitioned output.
    fn flush_pre_partitioned(&mut self) -> DaftResult<()> {
        if self.pre_repartitioned.is_empty() {
            return Ok(());
        }
        let pre_repartitioned = std::mem::take(&mut self.pre_repartitioned);
        self.pre_repartitioned_size_bytes = 0;

        let concated = RecordBatch::concat(pre_repartitioned)?;
        let num_partitions = self.num_partitions();

        let partitioned = match &self.repartition_spec {
            RepartitionSpec::Hash(_) => {
                concated.partition_by_hash(self.bound_keys.as_slice(), num_partitions)?
            }
            RepartitionSpec::Random(config) => {
                concated.partition_by_random(num_partitions, config.seed.unwrap_or(0))?
            }
            RepartitionSpec::Range(config) => {
                concated.partition_by_range(&config.by, &config.boundaries, &config.descending)?
            }
        };

        for (acc, part) in self.post_repartitioned.iter_mut().zip(partitioned) {
            acc.push(part);
        }
        Ok(())
    }
}

/// The repartition sink's backend: either one of the shared local shuffle
/// backends, or Celeborn.
///
/// Celeborn is deliberately kept out of [`LocalShuffleBackend`]: its state is
/// specific to this sink — a shuffle client, a once-per-(worker, shuffle)
/// registration latch, and the mapper-attempt registry — and no other local
/// shuffle sink pushes to Celeborn (a gather is translated into a
/// one-partition repartition; `IntoPartitions` is unsupported).
#[derive(Clone)]
enum RepartitionBackend {
    Local(LocalShuffleBackend),
    #[cfg(feature = "celeborn")]
    Celeborn {
        num_partitions: usize,
        shuffle_id: u64,
        num_mappers: u32,
        client: Arc<dyn CelebornClient>,
        registered: Arc<tokio::sync::OnceCell<()>>,
        /// Per-pipeline registry of mapper attempts (`map_id`/`attempt_id`),
        /// keyed by `InputId`. Populated by the execution loop at enqueue time;
        /// consulted in `make_state`.
        map_attempts: Option<MapperAttemptRegistry>,
    },
}

impl RepartitionBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Local(backend) => backend.name(),
            #[cfg(feature = "celeborn")]
            Self::Celeborn { .. } => "Celeborn",
        }
    }
}

fn repartition_buffer_threshold_bytes(
    backend: &RepartitionBackend,
    num_partitions: usize,
) -> usize {
    match backend {
        RepartitionBackend::Local(LocalShuffleBackend::Ray) => {
            REPARTITION_MAX_BUFFER_THRESHOLD_BYTES
        }
        RepartitionBackend::Local(LocalShuffleBackend::Flight(_)) => CHUNK_TARGET_BYTES
            .saturating_mul(num_partitions.max(1))
            .clamp(
                REPARTITION_MIN_BUFFER_THRESHOLD_BYTES,
                REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
            ),
        #[cfg(feature = "celeborn")]
        RepartitionBackend::Celeborn { .. } => REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
    }
}

pub struct RepartitionSink {
    backend: RepartitionBackend,
    schema: SchemaRef,
    repartition_spec: RepartitionSpec,
    bound_keys: Vec<BoundExpr>,
    num_partitions: usize,
}

impl RepartitionSink {
    pub fn new(
        schema: SchemaRef,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        backend: LocalShuffleBackend,
    ) -> DaftResult<Self> {
        Self::with_backend(
            schema,
            repartition_spec,
            num_partitions,
            RepartitionBackend::Local(backend),
        )
    }

    #[cfg(feature = "celeborn")]
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_celeborn(
        num_partitions: usize,
        shuffle_id: u64,
        num_mappers: u32,
        repartition_spec: RepartitionSpec,
        client: Arc<dyn CelebornClient>,
        schema: SchemaRef,
        map_attempts: Option<MapperAttemptRegistry>,
    ) -> DaftResult<Self> {
        Self::with_backend(
            schema,
            repartition_spec,
            num_partitions,
            RepartitionBackend::Celeborn {
                num_partitions,
                shuffle_id,
                num_mappers,
                client,
                registered: Arc::new(tokio::sync::OnceCell::new()),
                map_attempts,
            },
        )
    }

    fn with_backend(
        schema: SchemaRef,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        backend: RepartitionBackend,
    ) -> DaftResult<Self> {
        let bound_keys = match &repartition_spec {
            RepartitionSpec::Hash(config) => BoundExpr::bind_all(&config.by, &schema)?,
            RepartitionSpec::Random(_) | RepartitionSpec::Range(_) => Vec::new(),
        };
        Ok(Self {
            backend,
            schema,
            repartition_spec,
            bound_keys,
            num_partitions,
        })
    }
}

/// Per-mapper state for the Celeborn backend.
#[cfg(feature = "celeborn")]
pub(crate) struct CelebornRepartitionState {
    client: Arc<dyn CelebornClient>,
    shuffle_id: u64,
    map_id: u32,
    attempt_id: u32,
    num_mappers: u32,
    registered: Arc<tokio::sync::OnceCell<()>>,
    rows_per_partition: Vec<usize>,
    bytes_per_partition: Vec<usize>,
    num_partitions: u32,
    /// Raw morsels buffered until they cross the sink's byte threshold, then
    /// fused and partitioned in one pass (see [`flush_pre_partitioned`]).
    /// Buffering amortizes the partition pass instead of partitioning every
    /// 1024-row morsel.
    pre_repartitioned: Vec<MicroPartition>,
    pre_repartitioned_size_bytes: usize,
    /// Per-partition accumulator: each `partition_by_hash` slice is appended to
    /// its partition's bucket here. A
    /// partition is pushed to Celeborn as one accumulated block — either eagerly
    /// once it reaches
    /// [`CHUNK_TARGET_BYTES`] (streaming, keeps push chunks well-sized), under
    /// global memory pressure, or at `finalize`. Accumulating per partition (vs
    /// pushing every flush's `threshold / num_partitions` slice) is what avoids
    /// the high-`num_partitions` tiny-chunk storm (1-row pushes / 1-row reads).
    post_repartitioned: Vec<Vec<MicroPartition>>,
    /// Bytes currently buffered per partition (drives the per-partition eager
    /// push at `CHUNK_TARGET_BYTES`).
    post_partition_bytes: Vec<usize>,
    /// Total bytes buffered across all partitions (drives the global
    /// memory-pressure push that bounds this mapper's footprint).
    post_total_bytes: usize,
    /// Base seed for random repartition, threaded verbatim from
    /// `RandomShuffleConfig.seed` (`None` when unseeded). Combined with
    /// `map_id`/`attempt_id` and [`Self::flush_idx`] to derive a distinct seed
    /// per flush.
    random_seed: Option<u64>,
    /// Number of flushes this mapper has done, mixed into the per-flush random
    /// seed. A counter rather than a clock reading so that a seeded random
    /// repartition stays reproducible.
    flush_idx: u64,
}

/// Fuse the buffered raw morsels, partition once, and append each partition's
/// slice to its `post_repartitioned` bucket (accumulate — does NOT push).
/// Clears the raw buffer. Called from `sink` at the pre-flush threshold and from
/// `finalize` to drain the tail before pushing.
#[cfg(feature = "celeborn")]
fn flush_pre_partitioned(
    state: &mut CelebornRepartitionState,
    bound_keys: &[BoundExpr],
    num_partitions: usize,
) -> DaftResult<()> {
    if state.pre_repartitioned.is_empty() {
        return Ok(());
    }
    let buffered = std::mem::take(&mut state.pre_repartitioned);
    state.pre_repartitioned_size_bytes = 0;

    let concated = MicroPartition::concat(buffered)?;
    let partitioned = if bound_keys.is_empty() {
        // Vary the seed per (map_id, attempt_id, flush) so each flush scatters
        // rows into an independent subset of partitions. A fixed seed would
        // reuse one sequence across every flush and every mapper, so only the
        // ~L distinct indices hit within a single flush would ever receive data
        // (L = rows per flush), leaving the remaining partitions empty.
        //
        // The flush counter — not a clock reading — is what distinguishes the
        // flushes, so a run with `RandomShuffleConfig.seed` set stays
        // reproducible; `map_id`/`attempt_id` decorrelate mappers from each
        // other. The counter is spread across the whole word before being
        // folded in, since consecutive small integers would otherwise differ
        // only in the low bits.
        state.flush_idx += 1;
        let seed = state.random_seed.unwrap_or(0)
            ^ ((state.map_id as u64) << 40)
            ^ ((state.attempt_id as u64) << 32)
            ^ state.flush_idx.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        concated.partition_by_random(num_partitions, seed)?
    } else {
        concated.partition_by_hash(bound_keys, num_partitions)?
    };

    for (partition_idx, mp) in partitioned.into_iter().enumerate() {
        if mp.is_empty() {
            continue;
        }
        let mp_bytes = mp.size_bytes();
        state.post_repartitioned[partition_idx].push(mp);
        state.post_partition_bytes[partition_idx] += mp_bytes;
        state.post_total_bytes += mp_bytes;
    }
    Ok(())
}

/// Push one partition's accumulated block to Celeborn as a single Arrow-IPC
/// stream. Bounding the per-push payload and merging small pushes is the Celeborn
/// client's responsibility (it batches up to `celeborn.client.push.buffer.max.size`
/// and merges across partitions before hitting the wire), so the map side does
/// not split here. It hands over exactly one accumulated block per partition and
/// frees that partition's buffered memory.
#[cfg(feature = "celeborn")]
async fn push_one_celeborn_partition(
    state: &mut CelebornRepartitionState,
    partition_idx: usize,
) -> DaftResult<()> {
    let parts = std::mem::take(&mut state.post_repartitioned[partition_idx]);
    let freed = state.post_partition_bytes[partition_idx];
    state.post_partition_bytes[partition_idx] = 0;
    state.post_total_bytes = state.post_total_bytes.saturating_sub(freed);
    if parts.is_empty() {
        return Ok(());
    }
    let mp = MicroPartition::concat(parts)?;
    let num_rows = mp.len();
    if num_rows == 0 {
        return Ok(());
    }
    let ipc_bytes = mp.write_to_ipc_stream()?;
    state
        .client
        .push_data(
            state.shuffle_id,
            state.map_id,
            state.attempt_id,
            partition_idx as u32,
            &ipc_bytes,
        )
        .await?;
    state.rows_per_partition[partition_idx] += num_rows;
    state.bytes_per_partition[partition_idx] += ipc_bytes.len();
    Ok(())
}

/// After a flush, push (a) any partition that reached `CHUNK_TARGET_BYTES`
/// (eager, well-sized, streams while the map keeps computing), then (b) if the
/// mapper's total buffered bytes still exceed `memory_cap`, push everything to
/// relieve memory. Under (b) the per-partition blocks may be small (this is the
/// bounded-memory floor of `memory_cap / num_partitions` at high partition
/// counts), but the mapper's footprint stays bounded.
#[cfg(feature = "celeborn")]
async fn drain_ready_celeborn_partitions(
    state: &mut CelebornRepartitionState,
    memory_cap: usize,
) -> DaftResult<()> {
    let num_partitions = state.post_repartitioned.len();
    for partition_idx in 0..num_partitions {
        if state.post_partition_bytes[partition_idx] >= CHUNK_TARGET_BYTES {
            push_one_celeborn_partition(state, partition_idx).await?;
        }
    }
    if state.post_total_bytes >= memory_cap {
        for partition_idx in 0..num_partitions {
            push_one_celeborn_partition(state, partition_idx).await?;
        }
    }
    Ok(())
}

/// Push all remaining accumulated partitions (used at `finalize`).
#[cfg(feature = "celeborn")]
async fn push_all_celeborn_partitions(state: &mut CelebornRepartitionState) -> DaftResult<()> {
    for partition_idx in 0..state.post_repartitioned.len() {
        push_one_celeborn_partition(state, partition_idx).await?;
    }
    Ok(())
}

pub(crate) enum RepartitionSinkState {
    Acc(RepartitionAccState),
    #[cfg(feature = "celeborn")]
    Celeborn(CelebornRepartitionState),
}

impl BlockingSink for RepartitionSink {
    type State = RepartitionSinkState;

    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        match state {
            RepartitionSinkState::Acc(mut acc_state) => {
                let buffer_threshold_bytes =
                    repartition_buffer_threshold_bytes(&self.backend, self.num_partitions);
                spawner
                    .spawn(
                        async move {
                            let input_bytes = input.size_bytes();
                            acc_state.pre_repartitioned_size_bytes += input_bytes;
                            acc_state
                                .pre_repartitioned
                                .extend(input.record_batches().iter().cloned());

                            if acc_state.pre_repartitioned_size_bytes >= buffer_threshold_bytes {
                                acc_state.flush_pre_partitioned()?;
                            }

                            Ok(RepartitionSinkState::Acc(acc_state))
                        },
                        Span::current(),
                    )
                    .into()
            }
            #[cfg(feature = "celeborn")]
            RepartitionSinkState::Celeborn(mut state) => {
                let num_partitions = self.num_partitions;
                let bound_keys = self.bound_keys.clone();
                // Partition the raw buffer in modest batches (16MB) so per-partition
                // accumulators can grow across many batches. What is bounded is the
                // mapper's total accumulated footprint (`memory_cap`), not any single
                // partition, so a skewed key still builds up blocks worth pushing.
                let pre_flush_bytes = REPARTITION_MIN_BUFFER_THRESHOLD_BYTES;
                let memory_cap =
                    repartition_buffer_threshold_bytes(&self.backend, self.num_partitions);

                spawner
                    .spawn(
                        async move {
                            // Register exactly once per (worker, shuffle) AND make
                            // every mapper task await that registration before it
                            // pushes. A plain AtomicBool swap only guarantees "someone
                            // is registering", not "registration completed": a second
                            // task could skip the swap and push_data to a not-yet-
                            // registered shuffle. OnceCell::get_or_try_init runs the
                            // registration on the first caller and blocks all others
                            // until it finishes, so every push happens-after register.
                            {
                                let client = state.client.clone();
                                let shuffle_id = state.shuffle_id;
                                let num_mappers = state.num_mappers;
                                let num_partitions_u32 = state.num_partitions;
                                state
                                    .registered
                                    .get_or_try_init(|| async move {
                                        client
                                            .register_shuffle(
                                                shuffle_id,
                                                num_mappers,
                                                num_partitions_u32,
                                            )
                                            .await
                                    })
                                    .await?;
                            }

                            // Buffer raw morsels; at the pre-flush threshold, partition
                            // once and ACCUMULATE each partition's slice. Then push
                            // partitions that reached CHUNK_TARGET_BYTES (streams while
                            // computing, well-sized chunks) and, under memory pressure,
                            // push the rest to keep the footprint bounded. A partition
                            // is emitted as one accumulated block rather than a
                            // ~threshold/num_partitions slice per flush.
                            state.pre_repartitioned_size_bytes += input.size_bytes();
                            state.pre_repartitioned.push(input);

                            if state.pre_repartitioned_size_bytes >= pre_flush_bytes {
                                flush_pre_partitioned(&mut state, &bound_keys, num_partitions)?;
                                drain_ready_celeborn_partitions(&mut state, memory_cap).await?;
                            }

                            Ok(RepartitionSinkState::Celeborn(state))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();

        let backend = match &self.backend {
            RepartitionBackend::Local(backend) => backend.clone(),
            #[cfg(feature = "celeborn")]
            RepartitionBackend::Celeborn { .. } => {
                let mut celeborn_states: Vec<CelebornRepartitionState> = states
                    .into_iter()
                    .map(|s| match s {
                        RepartitionSinkState::Celeborn(cs) => cs,
                        _ => panic!("RepartitionSink state/backend mismatch"),
                    })
                    .collect();
                let bound_keys = self.bound_keys.clone();

                return spawner
                    .spawn(
                        async move {
                            // 1. Partition each mapper's tail raw buffer into its
                            //    post_repartitioned accumulator (no rows lost).
                            for state in &mut celeborn_states {
                                flush_pre_partitioned(state, &bound_keys, num_partitions)?;
                            }

                            // 2. Push each mapper's accumulated partitions — one
                            //    block per partition (payload sizing left to the
                            //    Celeborn client). All pushes must complete before
                            //    any mapper_end.
                            for state in &mut celeborn_states {
                                push_all_celeborn_partitions(state).await?;
                            }

                            let mut seen_mappers = std::collections::HashSet::new();
                            for state in &celeborn_states {
                                if seen_mappers.insert((
                                    state.shuffle_id,
                                    state.map_id,
                                    state.attempt_id,
                                )) {
                                    state
                                        .client
                                        .mapper_end(
                                            state.shuffle_id,
                                            state.map_id,
                                            state.attempt_id,
                                        )
                                        .await?;
                                }
                            }

                            let mut rows_per_partition = vec![0usize; num_partitions];
                            let mut bytes_per_partition = vec![0usize; num_partitions];
                            for state in celeborn_states {
                                for (i, count) in state.rows_per_partition.iter().enumerate() {
                                    rows_per_partition[i] += *count;
                                }
                                for (i, count) in state.bytes_per_partition.iter().enumerate() {
                                    bytes_per_partition[i] += *count;
                                }
                            }

                            Ok(BlockingSinkOutput::ShufflePartitionMetas(
                                rows_per_partition
                                    .into_iter()
                                    .zip(bytes_per_partition)
                                    .map(|(num_rows, size_bytes)| {
                                        ShufflePartitionMeta::new(num_rows, size_bytes)
                                    })
                                    .collect(),
                            ))
                        },
                        Span::current(),
                    )
                    .into();
            }
        };

        let acc_states: Vec<RepartitionAccState> = states
            .into_iter()
            .map(|s| match s {
                RepartitionSinkState::Acc(acc) => acc,
                #[cfg(feature = "celeborn")]
                _ => panic!("RepartitionSink state/backend mismatch"),
            })
            .collect();

        spawner
            .spawn(
                async move {
                    let mut states = acc_states;
                    states
                        .iter_mut()
                        .try_for_each(RepartitionAccState::flush_pre_partitioned)?;
                    let (per_partition, input_id) =
                        flatten_per_partition(states, num_partitions, schema.clone())?;

                    match backend {
                        LocalShuffleBackend::Ray => {
                            let mut joinset = OrderedJoinSet::new();
                            for data in per_partition {
                                joinset.spawn(async move {
                                    let concated_rb = data.concat_or_get()?;
                                    let mp = MicroPartition::new_loaded(
                                        data.schema(),
                                        Arc::new(concated_rb.into_iter().collect()),
                                        None,
                                    );
                                    Ok::<_, DaftError>(mp)
                                });
                            }
                            let mut partitions = Vec::with_capacity(num_partitions);
                            while let Some(output) = joinset.join_next().await {
                                partitions.push(output??);
                            }
                            Ok(BlockingSinkOutput::Partitions(partitions))
                        }
                        LocalShuffleBackend::Flight(ctx) => {
                            let compression = parse_compression(ctx.compression.as_deref())?;
                            let partition_caches = write_partitions_one_shot(
                                input_id,
                                ctx.shuffle_id,
                                &ctx.shuffle_dirs,
                                schema,
                                compression,
                                per_partition,
                            )
                            .await?;

                            ctx.local_server
                                .register_shuffle_partitions(
                                    ctx.shuffle_id,
                                    partition_caches.clone(),
                                )
                                .await?;
                            Ok(BlockingSinkOutput::FlightPartitionRefs(
                                partition_caches
                                    .into_iter()
                                    .map(|partition| FlightPartitionRef {
                                        shuffle_id: ctx.shuffle_id,
                                        server_address: ctx.shuffle_address.clone(),
                                        partition_ref_id: partition.partition_ref_id,
                                        num_rows: partition.num_rows,
                                        size_bytes: partition.size_bytes,
                                    })
                                    .collect(),
                            ))
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("Repartition({})", self.backend.name()).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        let backend_name = self.backend.name();
        match &self.repartition_spec {
            RepartitionSpec::Hash(config) => vec![format!(
                "Repartition({backend_name}): By {} into {} partitions",
                config.by.iter().map(|e| e.to_string()).join(", "),
                self.num_partitions
            )],
            RepartitionSpec::Random(_) => vec![format!(
                "Repartition({backend_name}): Random into {} partitions",
                self.num_partitions
            )],
            RepartitionSpec::Range(_) => vec![format!(
                "Repartition({backend_name}): Range into {} partitions",
                self.num_partitions
            )],
        }
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            RepartitionBackend::Local(_) => {
                Ok(RepartitionSinkState::Acc(RepartitionAccState::new(
                    self.num_partitions,
                    input_id,
                    self.bound_keys.clone(),
                    self.repartition_spec.clone(),
                )))
            }
            #[cfg(feature = "celeborn")]
            RepartitionBackend::Celeborn {
                num_partitions,
                shuffle_id,
                num_mappers,
                client,
                registered,
                map_attempts,
            } => {
                // `map_id` must be the upstream partition ordinal, in
                // `[0, num_mappers)`, and
                // `attempt_id` must increment across reschedules — both
                // supplied per-task at enqueue time via the mapper-attempt
                // registry, keyed by `input_id`. With no registry there is no
                // coordinator assigning ordinals either — a single-process run,
                // where `input_id` already is the ordinal — so fall back to it.
                let (map_id, attempt_id) = map_attempts
                    .as_ref()
                    .and_then(|r| r.lock().ok().and_then(|m| m.get(&input_id).copied()))
                    .map(|a| (a.map_id, a.attempt_id))
                    .unwrap_or((input_id, 0));
                // Hard error (not just `debug_assert`) so a missing/incorrect
                // `map_id` cannot silently push out-of-range data to Celeborn in
                // release builds: Celeborn requires `map_id ∈ [0, num_mappers)`.
                if map_id >= *num_mappers {
                    return Err(DaftError::InternalError(format!(
                        "Celeborn map_id={map_id} out of range [0, {num_mappers}) for \
                         input_id={input_id}: per-task shuffle coordinates are missing or \
                         inconsistent (coordinator must inject `map_id` for every mapper)"
                    )));
                }
                // Preserve the RandomShuffleConfig seed as the reproducible base;
                // only meaningful for the random (keyless) path.
                let random_seed = match &self.repartition_spec {
                    RepartitionSpec::Random(config) => config.seed,
                    _ => None,
                };
                Ok(RepartitionSinkState::Celeborn(CelebornRepartitionState {
                    client: client.clone(),
                    shuffle_id: *shuffle_id,
                    map_id,
                    attempt_id,
                    num_mappers: *num_mappers,
                    registered: registered.clone(),
                    rows_per_partition: vec![0; *num_partitions],
                    bytes_per_partition: vec![0; *num_partitions],
                    num_partitions: *num_partitions as u32,
                    pre_repartitioned: Vec::new(),
                    pre_repartitioned_size_bytes: 0,
                    post_repartitioned: (0..*num_partitions).map(|_| Vec::new()).collect(),
                    post_partition_bytes: vec![0; *num_partitions],
                    post_total_bytes: 0,
                    random_seed,
                    flush_idx: 0,
                }))
            }
        }
    }
}

fn flatten_per_partition(
    mut states: Vec<RepartitionAccState>,
    num_partitions: usize,
    schema: SchemaRef,
) -> DaftResult<(Vec<MicroPartition>, InputId)> {
    let input_id = states
        .first()
        .map(|s| s.input_id)
        .expect("RepartitionSink::finalize called with no states");
    debug_assert!(states.iter().all(|s| s.input_id == input_id));
    debug_assert!(
        states
            .iter()
            .all(|s| s.post_repartitioned.len() == num_partitions)
    );

    let per_partition = (0..num_partitions)
        .map(|partition_idx| {
            let chunks = states
                .iter_mut()
                .flat_map(|state| std::mem::take(&mut state.post_repartitioned[partition_idx]))
                .collect::<Vec<_>>();
            MicroPartition::new_loaded(schema.clone(), Arc::new(chunks), None)
        })
        .collect::<Vec<_>>();

    Ok((per_partition, input_id))
}

fn parse_compression(s: Option<&str>) -> DaftResult<Option<arrow_ipc::CompressionType>> {
    match s {
        None | Some("") | Some("none") => Ok(None),
        Some("lz4") => Ok(Some(arrow_ipc::CompressionType::LZ4_FRAME)),
        Some("zstd") => Ok(Some(arrow_ipc::CompressionType::ZSTD)),
        Some(other) => Err(DaftError::ValueError(format!(
            "Unsupported compression for shuffle IPC writer: {}, only lz4 and zstd are supported",
            other
        ))),
    }
}
