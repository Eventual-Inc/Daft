use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::{
    multi_partition_cache::repartition_agg, server::flight_server::ShuffleFlightServer,
    shuffle_cache::{PartitionCache, partition_ref_id},
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

/// Per-(worker, input) accumulator. One `Vec<MicroPartition>` per output partition.
/// Used identically by both backends — the only difference is what `finalize` does
/// with the accumulated data.
pub(crate) struct RepartitionAccState {
    /// `per_partition[i]` holds all inputs from this worker destined for output partition `i`.
    per_partition: Vec<Vec<MicroPartition>>,
    /// Propagated from `make_state` so the flight finalize path can name the output file.
    /// All worker states for a single `BlockingSink::finalize` call share the same input_id.
    input_id: InputId,
}

impl RepartitionAccState {
    fn new(num_partitions: usize, input_id: InputId) -> Self {
        Self {
            per_partition: (0..num_partitions).map(|_| Vec::new()).collect(),
            input_id,
        }
    }

    fn push(&mut self, parts: Vec<MicroPartition>) {
        debug_assert_eq!(parts.len(), self.per_partition.len());
        for (acc, part) in self.per_partition.iter_mut().zip(parts) {
            acc.push(part);
        }
    }
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
}

impl RepartitionSink {
    pub fn new_ray(repartition_spec: RepartitionSpec, num_partitions: usize) -> Self {
        Self {
            backend: RepartitionBackend::Ray,
            repartition_spec,
            num_partitions,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_flight(
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_id: u64,
        repartition_spec: RepartitionSpec,
        shuffle_dirs: Vec<String>,
        _compression: Option<String>, // TODO: re-introduce when oneshot writer supports it
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
            },
            repartition_spec,
            num_partitions,
        })
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

        spawner
            .spawn(
                async move {
                    use std::sync::atomic::Ordering;
                    use std::time::Instant;
                    let t_sink = Instant::now();
                    let input_rows = input.len() as u64;
                    let input_bytes = input.size_bytes() as u64;
                    repartition_agg::SINK_INPUT_ROWS.fetch_add(input_rows, Ordering::Relaxed);
                    repartition_agg::SINK_INPUT_BYTES.fetch_add(input_bytes, Ordering::Relaxed);

                    let t_partition = Instant::now();
                    let partitioned = match repartition_spec {
                        RepartitionSpec::Hash(config) => {
                            let bound_exprs = BoundExpr::bind_all(&config.by, &input.schema())?;
                            input.partition_by_hash(&bound_exprs, num_partitions)?
                        }
                        RepartitionSpec::Random(config) => {
                            input.partition_by_random(num_partitions, config.seed.unwrap_or(0))?
                        }
                        RepartitionSpec::Range(config) => input.partition_by_range(
                            &config.by,
                            &config.boundaries,
                            &config.descending,
                        )?,
                    };
                    repartition_agg::PARTITION_CALLS.fetch_add(1, Ordering::Relaxed);
                    repartition_agg::PARTITION_US.fetch_add(
                        t_partition.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );

                    // Pure in-memory push — no async I/O, no channel sends, no flush
                    // thresholds. Matches the ray path: each output partition's Vec
                    // grows by one MicroPartition reference per sink call.
                    state.push(partitioned);

                    repartition_agg::SINK_CALLS.fetch_add(1, Ordering::Relaxed);
                    repartition_agg::SINK_US.fetch_add(
                        t_sink.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;

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
            } => {
                let shuffle_id = *shuffle_id;
                let shuffle_dirs = shuffle_dirs.clone();
                let local_server = local_server.clone();
                let shuffle_address = shuffle_address.clone();
                let schema = schema.clone();
                spawner
                    .spawn(
                        async move {
                            use std::sync::atomic::Ordering;
                            use std::time::Instant;
                            let t_close = Instant::now();
                            let partition_caches = write_per_partition_append(
                                input_id,
                                shuffle_id,
                                &shuffle_dirs,
                                &schema,
                                &local_server,
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

/// Concat each output partition's `Vec<MicroPartition>` in parallel, then append each
/// non-empty partition's IPC message to the per-`(shuffle_id, partition_idx)` shared
/// file managed by `local_server`. Returns one `PartitionCache` per output partition
/// pointing at the shared file with the byte range this map task occupies.
async fn write_per_partition_append(
    input_id: InputId,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: &SchemaRef,
    local_server: &Arc<ShuffleFlightServer>,
    partitions_per_output: Vec<Vec<MicroPartition>>,
) -> DaftResult<Vec<PartitionCache>> {
    let num_partitions = partitions_per_output.len();

    // Phase 1 (parallel): concat each output partition's MicroPartitions into a single
    // arrow batch, no I/O. Empty partitions stay as None.
    let mut concat_futs = Vec::with_capacity(num_partitions);
    for parts in partitions_per_output {
        concat_futs.push(tokio::spawn(async move { concat_one_partition(parts) }));
    }
    let concated_results = futures::future::join_all(concat_futs).await;
    let mut concated_per_partition: Vec<Option<(usize, usize, arrow_array::RecordBatch)>> =
        Vec::with_capacity(num_partitions);
    for jr in concated_results {
        let inner = jr.map_err(|e| common_error::DaftError::InternalError(e.to_string()))?;
        concated_per_partition.push(inner?);
    }

    // Phase 2 (per-partition): get/create the shared writer for this partition's file
    // and append our IPC message. The writer's mutex serializes concurrent map tasks
    // writing to the same file; concat work has already happened.
    let mut caches = Vec::with_capacity(num_partitions);
    for (partition_idx, slot) in concated_per_partition.into_iter().enumerate() {
        let ref_id = partition_ref_id(input_id, partition_idx);
        let (num_rows, size_bytes, byte_ranges, file_paths, bytes_per_file) = match slot {
            Some((rows, bytes, arrow_batch)) => {
                let writer = local_server
                    .get_or_create_partition_writer(
                        shuffle_id,
                        partition_idx,
                        shuffle_dirs,
                        schema,
                    )
                    .await?;
                let (before, after) = writer.write_batch(&arrow_batch).await?;
                (
                    rows,
                    bytes,
                    Some(vec![(before, after)]),
                    vec![writer.file_path.clone()],
                    vec![(after - before) as usize],
                )
            }
            None => (0, 0, Some(Vec::new()), Vec::new(), Vec::new()),
        };
        caches.push(PartitionCache {
            partition_ref_id: ref_id,
            schema: schema.clone(),
            bytes_per_file,
            file_paths,
            num_rows,
            size_bytes,
            byte_ranges,
        });
    }
    Ok(caches)
}

/// Concat one output partition's MicroPartitions into a single arrow RecordBatch.
/// Returns `Some((num_rows, size_bytes, batch))` if non-empty, else `None`. Identical
/// to the helper inside `oneshot_writer.rs` — duplicated to keep this experiment
/// self-contained.
fn concat_one_partition(
    parts: Vec<MicroPartition>,
) -> DaftResult<Option<(usize, usize, arrow_array::RecordBatch)>> {
    if parts.is_empty() {
        return Ok(None);
    }
    let total_rows: usize = parts.iter().map(|p| p.len()).sum();
    if total_rows == 0 {
        return Ok(None);
    }
    let size_bytes: usize = parts.iter().map(|p| p.size_bytes()).sum();
    let combined = MicroPartition::concat(parts)?;
    let concated = combined.concat_or_get()?;
    match concated {
        Some(rb) => {
            let arrow_batch: arrow_array::RecordBatch = rb.try_into()?;
            Ok(Some((total_rows, size_bytes, arrow_batch)))
        }
        None => Ok(None),
    }
}
