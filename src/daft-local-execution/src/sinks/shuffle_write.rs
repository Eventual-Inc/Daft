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
    multi_partition_cache::repartition_agg,
    oneshot_writer::{write_partitions_one_shot, write_unpartitioned_one_shot},
    parse_flight_compression,
    server::flight_server::ShuffleFlightServer,
    shuffle_cache::partition_ref_id,
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

/// Per-(worker, input) accumulator for `ShuffleWriteSink`. Starts in
/// `Mode::Speculative` and may transition to `Mode::Partitioned` mid-stream
/// when buffered bytes cross the mode-switch threshold.
pub(crate) struct ShuffleWriteAccState {
    mode: Mode,
    input_id: InputId,
}

/// Speculative-vs-partitioned mode state. Owned data only — no schema, no
/// num_partitions; those live on the sink.
enum Mode {
    /// Buffering raw morsels; partition kernel has not run.
    Speculative {
        raw: Vec<MicroPartition>,
        bytes: usize,
    },
    /// Committed to per-partition writes. Mirrors `RepartitionAccState`'s
    /// shape; we re-implement the 4 MiB flush loop here rather than refactor
    /// `RepartitionSink` to avoid pulling its full struct into this state.
    Partitioned {
        per_partition: Vec<Vec<MicroPartition>>,
        buffered: Vec<MicroPartition>,
        buffered_bytes: usize,
    },
}

/// Per-partition flush threshold during Partitioned mode. Matches the value
/// in `sinks/repartition.rs`.
const REPARTITION_BUFFER_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

impl ShuffleWriteAccState {
    fn new(input_id: InputId) -> Self {
        Self {
            mode: Mode::Speculative {
                raw: Vec::new(),
                bytes: 0,
            },
            input_id,
        }
    }
}

pub struct ShuffleWriteSink {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    /// Threshold over accumulated bytes that triggers a transition from
    /// Speculative → Partitioned. Up to this much may live in memory before
    /// we commit to partitioning on the worker.
    mode_switch_threshold_bytes: usize,
    /// Server-side raw accumulator threshold; passed through to the Flight
    /// server via `register_shuffle_spec`.
    server_repartition_threshold_bytes: usize,
    /// Flight-side config.
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    local_server: Arc<ShuffleFlightServer>,
    shuffle_address: String,
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    /// Lazily-bound key projection for Hash repartitions; cached for the
    /// lifetime of the sink.
    bound_keys: Arc<OnceLock<Arc<Vec<BoundExpr>>>>,
    /// Cached bytes-per-row estimate to skip the `size_bytes()` walk on every
    /// `sink()` call (see `RepartitionSink` for the same pattern).
    bytes_per_row: OnceLock<f64>,
}

impl ShuffleWriteSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_id: u64,
        repartition_spec: RepartitionSpec,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        mode_switch_threshold_bytes: usize,
        server_repartition_threshold_bytes: usize,
    ) -> DaftResult<Self> {
        Ok(Self {
            repartition_spec,
            num_partitions,
            mode_switch_threshold_bytes,
            server_repartition_threshold_bytes,
            shuffle_id,
            shuffle_dirs,
            local_server,
            shuffle_address,
            schema,
            compression: parse_flight_compression(compression.as_deref())?,
            bound_keys: Arc::new(OnceLock::new()),
            bytes_per_row: OnceLock::new(),
        })
    }

    fn estimate_size_bytes(&self, input: &MicroPartition) -> usize {
        if let Some(&bpr) = self.bytes_per_row.get() {
            return (input.len() as f64 * bpr).round() as usize;
        }
        let observed = input.size_bytes();
        if !input.is_empty() {
            let bpr = observed as f64 / input.len() as f64;
            let _ = self.bytes_per_row.set(bpr);
        }
        observed
    }

}

impl BlockingSink for ShuffleWriteSink {
    type State = ShuffleWriteAccState;

    #[instrument(skip_all, name = "ShuffleWriteSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let input_bytes = self.estimate_size_bytes(&input);
        let mode_switch_threshold = self.mode_switch_threshold_bytes;
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        let schema = self.schema.clone();
        let bound_keys = self.bound_keys.clone();

        spawner
            .spawn(
                async move {
                    match &mut state.mode {
                        Mode::Speculative { raw, bytes } => {
                            *bytes += input_bytes;
                            raw.push(input);
                            if *bytes >= mode_switch_threshold {
                                // Transition: drain raw through the partition kernel
                                // once, then enter Partitioned mode and continue
                                // streaming subsequent morsels via the 4 MiB flush loop.
                                let drained_raw = std::mem::take(raw);
                                let partitioned = partition_micropartitions(
                                    drained_raw,
                                    &repartition_spec,
                                    &schema,
                                    num_partitions,
                                    &bound_keys,
                                )?;
                                let mut per_partition: Vec<Vec<MicroPartition>> =
                                    (0..num_partitions).map(|_| Vec::new()).collect();
                                for (i, p) in partitioned.into_iter().enumerate() {
                                    per_partition[i].push(p);
                                }
                                state.mode = Mode::Partitioned {
                                    per_partition,
                                    buffered: Vec::new(),
                                    buffered_bytes: 0,
                                };
                            }
                        }
                        Mode::Partitioned {
                            per_partition,
                            buffered,
                            buffered_bytes,
                        } => {
                            *buffered_bytes += input_bytes;
                            buffered.push(input);
                            if *buffered_bytes >= REPARTITION_BUFFER_THRESHOLD_BYTES {
                                let drained_buffer = std::mem::take(buffered);
                                *buffered_bytes = 0;
                                let partitioned = partition_micropartitions(
                                    drained_buffer,
                                    &repartition_spec,
                                    &schema,
                                    num_partitions,
                                    &bound_keys,
                                )?;
                                for (i, p) in partitioned.into_iter().enumerate() {
                                    per_partition[i].push(p);
                                }
                            }
                        }
                    }
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "ShuffleWriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        let schema = self.schema.clone();
        let bound_keys = self.bound_keys.clone();
        let shuffle_id = self.shuffle_id;
        let shuffle_dirs = self.shuffle_dirs.clone();
        let shuffle_address = self.shuffle_address.clone();
        let local_server = self.local_server.clone();
        let compression = self.compression;
        let server_repartition_threshold = self.server_repartition_threshold_bytes;

        spawner
            .spawn(
                async move {
                    use std::sync::atomic::Ordering;
                    use std::time::Instant;
                    let t_close = Instant::now();

                    // Worker states all share one input_id (BlockingSink::finalize is
                    // called once per input_id with all worker states for that input).
                    let input_id = states
                        .first()
                        .map(|s| s.input_id)
                        .expect("ShuffleWriteSink::finalize called with no states");
                    debug_assert!(
                        states.iter().all(|s| s.input_id == input_id),
                        "All worker states in a single finalize call must share input_id",
                    );

                    // Decide global mode: if ANY worker state already transitioned to
                    // Partitioned, the entire finalize must run in Partitioned mode
                    // (we have per-partition data that needs the mega-file shape).
                    // Otherwise everything fits speculatively → write one raw file.
                    let any_partitioned = states
                        .iter()
                        .any(|s| matches!(s.mode, Mode::Partitioned { .. }));

                    if !any_partitioned {
                        // Small-mode: stitch all worker raw buffers and write one raw IPC.
                        let mut all_raw: Vec<MicroPartition> = Vec::new();
                        for s in states {
                            if let Mode::Speculative { raw, .. } = s.mode {
                                all_raw.extend(raw);
                            }
                        }
                        let outcome = write_unpartitioned_one_shot(
                            input_id,
                            shuffle_id,
                            &shuffle_dirs,
                            schema.clone(),
                            compression,
                            all_raw,
                        )
                        .await?;
                        repartition_agg::FINALIZE_CLOSE_US.fetch_add(
                            t_close.elapsed().as_micros() as u64,
                            Ordering::Relaxed,
                        );
                        repartition_agg::FINALIZE_CALLS.fetch_add(1, Ordering::Relaxed);

                        // Register the spec before the raw entry. Idempotent on
                        // the server — concurrent workers all skip after the
                        // first registration, and run_repartition will see the
                        // spec by the time it fires.
                        local_server
                            .register_shuffle_spec(
                                shuffle_id,
                                repartition_spec.clone(),
                                num_partitions,
                                schema.clone(),
                                compression,
                                shuffle_dirs.clone(),
                                server_repartition_threshold,
                            )
                            .await?;

                        // Register the raw entry with all `num_partitions` promise
                        // ref_ids so readers can later request each partition by id.
                        let promise_ref_ids: Vec<u64> = (0..num_partitions)
                            .map(|i| partition_ref_id(input_id, i))
                            .collect();
                        local_server
                            .register_raw_entry(
                                shuffle_id,
                                outcome.file_path.clone(),
                                outcome.file_bytes as usize,
                                promise_ref_ids.clone(),
                            )
                            .await?;

                        // Emit `num_partitions` promise refs so the orchestrator's
                        // transpose sees the expected fanout. The per-partition
                        // num_rows/size_bytes are estimated by even-splitting the
                        // raw file's totals — scheduler heuristics only need a
                        // ballpark, and the true per-partition sizes aren't
                        // knowable until the server runs the partition kernel.
                        let split_rows = outcome.total_rows / num_partitions.max(1);
                        let split_bytes = outcome.size_bytes / num_partitions.max(1);
                        let refs: Vec<FlightPartitionRef> = promise_ref_ids
                            .into_iter()
                            .map(|partition_ref_id| FlightPartitionRef {
                                shuffle_id,
                                server_address: shuffle_address.clone(),
                                partition_ref_id,
                                num_rows: split_rows,
                                size_bytes: split_bytes,
                            })
                            .collect();
                        return Ok(BlockingSinkOutput::FlightPartitionRefs(refs));
                    }

                    // Big-mode (or mixed): every worker state must be reduced to a
                    // `Vec<Vec<MicroPartition>>` of length num_partitions. For workers
                    // still in Speculative mode, partition their raw buffer once
                    // before merging.
                    let mut per_partition: Vec<Vec<MicroPartition>> =
                        (0..num_partitions).map(|_| Vec::new()).collect();
                    for state in states {
                        match state.mode {
                            Mode::Speculative { raw, .. } => {
                                if raw.is_empty() {
                                    continue;
                                }
                                let partitioned = partition_micropartitions(
                                    raw,
                                    &repartition_spec,
                                    &schema,
                                    num_partitions,
                                    &bound_keys,
                                )?;
                                for (i, p) in partitioned.into_iter().enumerate() {
                                    per_partition[i].push(p);
                                }
                            }
                            Mode::Partitioned {
                                per_partition: state_pp,
                                buffered,
                                ..
                            } => {
                                let mut state_pp = state_pp;
                                if !buffered.is_empty() {
                                    let partitioned = partition_micropartitions(
                                        buffered,
                                        &repartition_spec,
                                        &schema,
                                        num_partitions,
                                        &bound_keys,
                                    )?;
                                    for (i, p) in partitioned.into_iter().enumerate() {
                                        state_pp[i].push(p);
                                    }
                                }
                                for (i, mut chunk) in state_pp.into_iter().enumerate() {
                                    per_partition[i].append(&mut chunk);
                                }
                            }
                        }
                    }

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
                            .map(|p| FlightPartitionRef {
                                shuffle_id,
                                server_address: shuffle_address.clone(),
                                partition_ref_id: p.partition_ref_id,
                                num_rows: p.num_rows,
                                size_bytes: p.size_bytes,
                            })
                            .collect(),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "ShuffleWrite(Flight)".to_string().into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.repartition_spec {
            RepartitionSpec::Hash(config) => vec![format!(
                "ShuffleWrite(Flight): By {} into {} partitions",
                config.by.iter().map(|e| e.to_string()).join(", "),
                self.num_partitions
            )],
            RepartitionSpec::Random(_) => vec![format!(
                "ShuffleWrite(Flight): Random into {} partitions",
                self.num_partitions
            )],
            RepartitionSpec::Range(_) => vec![format!(
                "ShuffleWrite(Flight): Range into {} partitions (unsupported)",
                self.num_partitions
            )],
        }
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        Ok(ShuffleWriteAccState::new(input_id))
    }
}

/// Fuse a buffer of MicroPartitions and apply the configured partition kernel,
/// returning one MicroPartition per output partition. Used both during the
/// Speculative→Partitioned transition and on every 4 MiB flush in Partitioned
/// mode.
fn partition_micropartitions(
    buffered: Vec<MicroPartition>,
    repartition_spec: &RepartitionSpec,
    schema: &SchemaRef,
    num_partitions: usize,
    bound_keys: &OnceLock<Arc<Vec<BoundExpr>>>,
) -> DaftResult<Vec<MicroPartition>> {
    let mut all_chunks: Vec<RecordBatch> = Vec::new();
    for mp in buffered {
        all_chunks.extend(mp.record_batches().iter().cloned());
    }
    if all_chunks.is_empty() {
        return Ok((0..num_partitions)
            .map(|_| MicroPartition::empty(Some(schema.clone())))
            .collect());
    }
    let fused_batch = RecordBatch::concat(&all_chunks)?;
    let fused = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![fused_batch]), None);
    match repartition_spec {
        RepartitionSpec::Hash(config) => {
            let bound = bound_keys.get_or_init(|| {
                Arc::new(
                    BoundExpr::bind_all(&config.by, schema)
                        .expect("BoundExpr::bind_all should succeed on a stable schema"),
                )
            });
            fused.partition_by_hash(bound, num_partitions)
        }
        RepartitionSpec::Random(config) => {
            fused.partition_by_random(num_partitions, config.seed.unwrap_or(0))
        }
        RepartitionSpec::Range(_) => Err(common_error::DaftError::InternalError(
            "Range partitioning is not supported by ShuffleWriteSink".to_string(),
        )),
    }
}
