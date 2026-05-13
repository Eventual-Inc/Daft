use std::sync::{Arc, OnceLock};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    multi_partition_cache::repartition_agg, oneshot_writer::write_partitions_one_shot,
    parse_flight_compression, server::flight_server::ShuffleFlightServer,
};
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

/// Buffer threshold before flushing a partition pass. Chosen empirically via
/// `repartition_bench`: 4 MiB consistently wins across (small/medium/large) × (small/medium/large
/// partition count) sweeps. Larger thresholds regress on big morsels because the resulting
/// permutation+take output exceeds L2 cache; smaller thresholds give back the per-flush fixed
/// overhead we are trying to amortise.
const REPARTITION_BUFFER_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Per-(worker, input) accumulator. One `Vec<MicroPartition>` per output partition.
/// Used identically by both backends — the only difference is what `finalize` does
/// with the accumulated data.
///
/// Morsels are buffered until they cross [`REPARTITION_BUFFER_THRESHOLD_BYTES`], at which point
/// they are fused into a single RecordBatch and partitioned once. This amortises the per-call
/// hash/rem/permutation overheads and (more importantly) shrinks the number of small batches the
/// finalize concat has to fuse.
pub(crate) struct RepartitionAccState {
    /// `per_partition[i]` holds all inputs from this worker destined for output partition `i`.
    per_partition: Vec<Vec<MicroPartition>>,
    /// Pending morsels staged for the next partition pass.
    buffered: Vec<MicroPartition>,
    /// Cumulative in-memory size of `buffered`, used to decide when to flush.
    buffered_bytes: usize,
    /// Per-worker local counters; merged into the global `repartition_agg` atomics on
    /// flush rather than on every `sink()` call.
    local_sink_calls: u64,
    local_sink_input_rows: u64,
    local_sink_input_bytes: u64,
    local_sink_us: u64,
    /// Propagated from `make_state` so the flight finalize path can name the output file.
    /// All worker states for a single `BlockingSink::finalize` call share the same input_id.
    input_id: InputId,
}

impl RepartitionAccState {
    fn new(num_partitions: usize, input_id: InputId) -> Self {
        Self {
            per_partition: (0..num_partitions).map(|_| Vec::new()).collect(),
            buffered: Vec::new(),
            buffered_bytes: 0,
            local_sink_calls: 0,
            local_sink_input_rows: 0,
            local_sink_input_bytes: 0,
            local_sink_us: 0,
            input_id,
        }
    }

    fn push_parts(&mut self, parts: Vec<MicroPartition>) {
        debug_assert_eq!(parts.len(), self.per_partition.len());
        for (acc, part) in self.per_partition.iter_mut().zip(parts) {
            acc.push(part);
        }
    }

    /// Flush local sink counters into the global atomics. Called from `flush_buffer` (and
    /// finalize) so each `sink()` call only touches thread-local memory.
    fn flush_local_counters(&mut self) {
        use std::sync::atomic::Ordering;
        if self.local_sink_calls > 0 {
            repartition_agg::SINK_CALLS.fetch_add(self.local_sink_calls, Ordering::Relaxed);
            repartition_agg::SINK_INPUT_ROWS
                .fetch_add(self.local_sink_input_rows, Ordering::Relaxed);
            repartition_agg::SINK_INPUT_BYTES
                .fetch_add(self.local_sink_input_bytes, Ordering::Relaxed);
            repartition_agg::SINK_US.fetch_add(self.local_sink_us, Ordering::Relaxed);
            self.local_sink_calls = 0;
            self.local_sink_input_rows = 0;
            self.local_sink_input_bytes = 0;
            self.local_sink_us = 0;
        }
    }
}

/// Fuse buffered morsels into a single RecordBatch, partition once, and push into
/// `state.per_partition`. No-op if the buffer is empty.
///
/// `bound_keys` is the pre-bound key-column projection for `Hash` repartitions, cached on
/// the sink so we don't pay `BoundExpr::bind_all` on every flush.
fn flush_buffer(
    state: &mut RepartitionAccState,
    repartition_spec: &RepartitionSpec,
    bound_keys: &OnceLock<Arc<Vec<BoundExpr>>>,
    num_partitions: usize,
) -> DaftResult<()> {
    if state.buffered.is_empty() {
        return Ok(());
    }
    // mem::take preserves the underlying capacity for reuse on the next round.
    let buffered = std::mem::take(&mut state.buffered);
    state.buffered_bytes = 0;

    // Skip the intermediate MicroPartition::concat: collect RecordBatch refs directly
    // (Arc::clones, no data move) and concat once into a single batch. RecordBatch::concat
    // has a single-input fast path (clone-only) for the case where every buffered MP held a
    // single chunk and the buffer had one MP.
    let mut all_chunks: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<SchemaRef> = None;
    for mp in buffered {
        if schema.is_none() {
            schema = Some(mp.schema());
        }
        all_chunks.extend(mp.record_batches().iter().cloned());
    }
    let schema = match schema {
        Some(s) => s,
        None => return Ok(()),
    };
    if all_chunks.is_empty() {
        return Ok(());
    }
    let fused_batch = RecordBatch::concat(&all_chunks)?;
    let fused = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![fused_batch]), None);

    let partitioned = match repartition_spec {
        RepartitionSpec::Hash(config) => {
            // Cache the bind once per sink lifetime — schema is invariant across flushes.
            let bound = bound_keys.get_or_init(|| {
                Arc::new(
                    BoundExpr::bind_all(&config.by, &schema)
                        .expect("BoundExpr::bind_all should succeed on a stable schema"),
                )
            });
            fused.partition_by_hash(bound, num_partitions)?
        }
        RepartitionSpec::Random(config) => {
            fused.partition_by_random(num_partitions, config.seed.unwrap_or(0))?
        }
        RepartitionSpec::Range(config) => {
            fused.partition_by_range(&config.by, &config.boundaries, &config.descending)?
        }
    };
    state.push_parts(partitioned);
    state.flush_local_counters();
    Ok(())
}

// TODO: unify shuffle backends in all local operations
enum RepartitionBackend {
    Ray,
    Flight {
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        schema: SchemaRef,
        compression: Option<arrow_ipc::CompressionType>,
    },
}

impl RepartitionBackend {
    fn name(&self) -> &'static str {
        match &self {
            Self::Ray => "Ray",
            Self::Flight { .. } => "Flight",
        }
    }
}

pub struct RepartitionSink {
    backend: RepartitionBackend,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    /// Lazily-bound key projection for `Hash` repartitions. Initialised on the first
    /// `flush_buffer` call and shared across all worker states for this sink.
    /// Wrapped in `Arc` so the per-morsel `sink()` async closure can cheaply own a reference.
    bound_keys: Arc<OnceLock<Arc<Vec<BoundExpr>>>>,
    /// Cached estimate of bytes-per-row, used to skip the (non-free) `size_bytes()` walk on
    /// every `sink()` call. Initialised from the first morsel; thereafter we approximate
    /// buffer size as `morsel.len() * bytes_per_row`. The threshold is loose (4 MiB) so a
    /// rough estimate is fine.
    bytes_per_row: OnceLock<f64>,
}

impl RepartitionSink {
    pub fn new_ray(repartition_spec: RepartitionSpec, num_partitions: usize) -> Self {
        Self {
            backend: RepartitionBackend::Ray,
            repartition_spec,
            num_partitions,
            bound_keys: Arc::new(OnceLock::new()),
            bytes_per_row: OnceLock::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_flight(
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_id: u64,
        repartition_spec: RepartitionSpec,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
    ) -> DaftResult<Self> {
        Ok(Self {
            backend: RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                local_server,
                shuffle_address,
                schema,
                compression: parse_flight_compression(compression.as_deref())?,
            },
            repartition_spec,
            num_partitions,
            bound_keys: Arc::new(OnceLock::new()),
            bytes_per_row: OnceLock::new(),
        })
    }

    /// Cheap byte-size estimate for a morsel. Calls `size_bytes()` once (on first morsel
    /// across the sink) to seed `bytes_per_row`, then uses `len * cached_ratio` thereafter.
    /// The flush threshold is intentionally loose (4 MiB), so a ±2× estimate is fine.
    fn estimate_size_bytes(&self, input: &MicroPartition) -> usize {
        if let Some(&bpr) = self.bytes_per_row.get() {
            return (input.len() as f64 * bpr).round() as usize;
        }
        let observed = input.size_bytes();
        if input.len() > 0 {
            let bpr = observed as f64 / input.len() as f64;
            let _ = self.bytes_per_row.set(bpr);
        }
        observed
    }
}

impl BlockingSink for RepartitionSink {
    type State = RepartitionAccState;

    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let repartition_spec = self.repartition_spec.clone();
        let num_partitions = self.num_partitions;
        let input_bytes = self.estimate_size_bytes(&input);
        let bound_keys = self.bound_keys.clone();

        spawner
            .spawn(
                async move {
                    use std::sync::atomic::Ordering;
                    use std::time::Instant;
                    let t_sink = Instant::now();
                    let input_rows = input.len() as u64;

                    state.local_sink_input_rows += input_rows;
                    state.local_sink_input_bytes += input_bytes as u64;
                    state.buffered_bytes += input_bytes;
                    state.buffered.push(input);

                    if state.buffered_bytes >= REPARTITION_BUFFER_THRESHOLD_BYTES {
                        let t_partition = Instant::now();
                        flush_buffer(
                            &mut state,
                            &repartition_spec,
                            &bound_keys,
                            num_partitions,
                        )?;
                        repartition_agg::PARTITION_CALLS.fetch_add(1, Ordering::Relaxed);
                        repartition_agg::PARTITION_US.fetch_add(
                            t_partition.elapsed().as_micros() as u64,
                            Ordering::Relaxed,
                        );
                    }

                    state.local_sink_calls += 1;
                    state.local_sink_us += t_sink.elapsed().as_micros() as u64;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        let bound_keys = self.bound_keys.clone();

        // Flush any buffered morsels in each worker state before flattening, plus the
        // worker-local atomics counters (in case the buffer was empty and `flush_buffer`
        // wouldn't have published them).
        let flush_result = states.iter_mut().try_for_each(|s| {
            flush_buffer(s, &repartition_spec, &bound_keys, num_partitions)?;
            s.flush_local_counters();
            Ok::<(), common_error::DaftError>(())
        });
        if let Err(e) = flush_result {
            return spawner
                .spawn(async move { Err(e) }, Span::current())
                .into();
        }

        // Flatten worker states into per-output-partition Vec<MicroPartition>. All states
        // for a single BlockingSink::finalize call share the same input_id (the sink
        // framework calls finalize once per input_id with all that input's worker states).
        let (per_partition, input_id) = flatten_per_partition(states, num_partitions);

        match &self.backend {
            RepartitionBackend::Ray => {
                spawner
                    .spawn(
                        async move {
                            let mut outputs = Vec::with_capacity(num_partitions);
                            for data in per_partition {
                                let fut = tokio::spawn(async move {
                                    if data.is_empty() {
                                        // No input was ever sunk for this partition — emit
                                        // empty rather than failing inside MicroPartition::concat.
                                        return Ok(MicroPartition::empty(None));
                                    }
                                    let together = MicroPartition::concat(data)?;
                                    let schema = together.schema();
                                    let concated = together.concat_or_get()?;
                                    let mp = MicroPartition::new_loaded(
                                        schema,
                                        Arc::new(if let Some(t) = concated {
                                            vec![t]
                                        } else {
                                            vec![]
                                        }),
                                        None,
                                    );
                                    Ok::<_, common_error::DaftError>(mp)
                                });
                                outputs.push(fut);
                            }
                            let partitions = futures::future::try_join_all(outputs)
                                .await
                                .unwrap()
                                .into_iter()
                                .collect::<DaftResult<Vec<_>>>()?;
                            Ok(BlockingSinkOutput::Partitions(partitions))
                        },
                        Span::current(),
                    )
                    .into()
            }
            RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                local_server,
                shuffle_address,
                schema,
                compression,
            } => {
                let shuffle_id = *shuffle_id;
                let shuffle_dirs = shuffle_dirs.clone();
                let local_server = local_server.clone();
                let shuffle_address = shuffle_address.clone();
                let schema = schema.clone();
                let compression = *compression;
                spawner
                    .spawn(
                        async move {
                            use std::sync::atomic::Ordering;
                            use std::time::Instant;
                            let t_close = Instant::now();
                            let partition_caches = write_partitions_one_shot(
                                input_id,
                                shuffle_id,
                                &shuffle_dirs,
                                schema.clone(),
                                compression,
                                per_partition,
                            )
                            .await?;
                            repartition_agg::FINALIZE_CLOSE_US.fetch_add(
                                t_close.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );
                            repartition_agg::FINALIZE_CALLS.fetch_add(1, Ordering::Relaxed);

                            local_server
                                .register_shuffle_partitions(shuffle_id, partition_caches.clone())
                                .await?;
                            Ok(BlockingSinkOutput::FlightPartitionRefs(
                                partition_caches
                                    .into_iter()
                                    .map(|partition| FlightPartitionRef {
                                        shuffle_id,
                                        server_address: shuffle_address.clone(),
                                        partition_ref_id: partition.partition_ref_id,
                                        num_rows: partition.num_rows,
                                        size_bytes: partition.size_bytes,
                                    })
                                    .collect(),
                            ))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
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
        Ok(RepartitionAccState::new(self.num_partitions, input_id))
    }
}

/// Move per-worker per-partition accumulators into a single `Vec<Vec<MicroPartition>>`
/// indexed by output-partition id. Returns the input_id (assumed identical across states,
/// since `BlockingSink::finalize` is called once per input_id).
fn flatten_per_partition(
    states: Vec<RepartitionAccState>,
    num_partitions: usize,
) -> (Vec<Vec<MicroPartition>>, InputId) {
    let input_id = states
        .first()
        .map(|s| s.input_id)
        .expect("RepartitionSink::finalize called with no states");
    debug_assert!(
        states.iter().all(|s| s.input_id == input_id),
        "All worker states in a single finalize call must share the same input_id",
    );

    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    for state in states {
        for (i, mut chunk) in state.per_partition.into_iter().enumerate() {
            per_partition[i].append(&mut chunk);
        }
    }
    (per_partition, input_id)
}

