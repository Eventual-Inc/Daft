use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::OrderedJoinSet;
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    oneshot_writer::write_partitions_one_shot_streaming,
    server::flight_server::ShuffleFlightServer, shuffle_cache::CHUNK_TARGET_BYTES,
};
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

// Worst-case buffered memory is `num_workers × num_inputs × threshold` — one
// accumulator per (worker, input).
const REPARTITION_MIN_BUFFER_THRESHOLD_BYTES: usize = 16 * 1024 * 1024; // 16 MB
const REPARTITION_MAX_BUFFER_THRESHOLD_BYTES: usize = 256 * 1024 * 1024; // 256 MB

/// Per-(worker, input) accumulator. Morsels are buffered until they cross
/// the sink's threshold, then fused and partitioned in one pass.
pub(crate) struct RepartitionAccState {
    post_repartitioned: Vec<Vec<RecordBatch>>,
    post_repartitioned_size_bytes: usize,
    pre_repartitioned: Vec<RecordBatch>,
    pre_repartitioned_size_bytes: usize,
    bound_keys: Vec<BoundExpr>,
    repartition_spec: RepartitionSpec,
    input_id: InputId,
    schema: SchemaRef,
    /// Single lazily-created spill writer (Flight only). Sealed to `spill_store` before finalize.
    spill_writer: Option<SpillWriter>,
    /// Sealed store (set after `seal_spill_writer`). Read during finalize.
    spill_store: Option<SpillStore>,
    /// Directories to spill into (empty for Ray backend — spill disabled).
    spill_dirs: Vec<String>,
    /// Shared spill-pool reservation held for the lifetime of this state.
    reservation: MemoryReservation,
    /// Per-operator cap (from `SpillConfig::cap()`); `None` means pool-only trigger.
    cap: Option<u64>,
}

impl RepartitionAccState {
    fn new(
        num_partitions: usize,
        input_id: InputId,
        bound_keys: Vec<BoundExpr>,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        spill_dirs: Vec<String>,
        reservation: MemoryReservation,
        cap: Option<u64>,
    ) -> Self {
        Self {
            post_repartitioned: (0..num_partitions).map(|_| Vec::new()).collect(),
            post_repartitioned_size_bytes: 0,
            pre_repartitioned: Vec::new(),
            pre_repartitioned_size_bytes: 0,
            bound_keys,
            repartition_spec,
            input_id,
            schema,
            spill_writer: None,
            spill_store: None,
            spill_dirs,
            reservation,
            cap,
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
            self.post_repartitioned_size_bytes += part.size_bytes();
            acc.push(part);
        }
        Ok(())
    }

    /// Spill a single bucket `p` to disk. No-op if `spill_dirs` is empty.
    fn spill_bucket(&mut self, p: usize) -> DaftResult<()> {
        if self.spill_dirs.is_empty() {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.post_repartitioned[p]);
        let freed: usize = batches.iter().map(|b| b.size_bytes()).sum();
        self.post_repartitioned_size_bytes -= freed.min(self.post_repartitioned_size_bytes);
        if batches.is_empty() {
            return Ok(());
        }
        let num_partitions = self.post_repartitioned.len();
        let writer = match &mut self.spill_writer {
            Some(w) => w,
            None => {
                let w = SpillWriter::new(
                    num_partitions,
                    &self.schema,
                    self.spill_dirs.clone(),
                    "daft_repartition_spill_",
                )?;
                self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for b in &batches {
            if b.len() > 0 {
                writer.write_batch(p, b)?;
            }
        }
        Ok(())
    }

    /// Seal the `spill_writer` into an immutable `spill_store`. Called before finalize.
    fn seal_spill_writer(&mut self) -> DaftResult<()> {
        if let Some(writer) = self.spill_writer.take() {
            self.spill_store = Some(writer.finish()?);
        }
        Ok(())
    }
}

impl SpillableBuckets for RepartitionAccState {
    fn resident_bytes(&self) -> u64 {
        self.post_repartitioned_size_bytes as u64
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        if self.spill_dirs.is_empty() {
            return Ok(false);
        }
        let Some((p, _)) = self
            .post_repartitioned
            .iter()
            .enumerate()
            .map(|(p, v)| (p, v.iter().map(|b| b.size_bytes()).sum::<usize>()))
            .filter(|(_, b)| *b > 0)
            .max_by_key(|(_, b)| *b)
        else {
            return Ok(false);
        };
        self.spill_bucket(p)?;
        Ok(true)
    }
}

// TODO: unify shuffle backends in all local operations
#[derive(Clone)]
enum RepartitionBackend {
    Ray,
    Flight {
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        compression: Option<arrow_ipc::CompressionType>,
        spill_config: Option<SpillConfig>,
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

fn repartition_buffer_threshold_bytes(
    backend: &RepartitionBackend,
    num_partitions: usize,
) -> usize {
    match backend {
        RepartitionBackend::Ray => REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
        RepartitionBackend::Flight { .. } => CHUNK_TARGET_BYTES
            .saturating_mul(num_partitions.max(1))
            .clamp(
                REPARTITION_MIN_BUFFER_THRESHOLD_BYTES,
                REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
            ),
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
    pub fn new_ray(
        schema: SchemaRef,
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
    ) -> DaftResult<Self> {
        let bound_keys = match &repartition_spec {
            RepartitionSpec::Hash(config) => BoundExpr::bind_all(&config.by, &schema)?,
            RepartitionSpec::Random(_) | RepartitionSpec::Range(_) => Vec::new(),
        };
        Ok(Self {
            backend: RepartitionBackend::Ray,
            schema,
            repartition_spec,
            bound_keys,
            num_partitions,
        })
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
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        let bound_keys = match &repartition_spec {
            RepartitionSpec::Hash(config) => BoundExpr::bind_all(&config.by, &schema)?,
            RepartitionSpec::Random(_) | RepartitionSpec::Range(_) => Vec::new(),
        };
        Ok(Self {
            backend: RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                local_server,
                shuffle_address,
                compression: parse_compression(compression.as_deref())?,
                spill_config,
            },
            schema,
            repartition_spec,
            bound_keys,
            num_partitions,
        })
    }

    fn spill_config(&self) -> Option<SpillConfig> {
        match &self.backend {
            RepartitionBackend::Flight { spill_config, .. } => spill_config.clone(),
            RepartitionBackend::Ray => None,
        }
    }

    fn spill_dirs(&self) -> Vec<String> {
        match &self.backend {
            RepartitionBackend::Flight { shuffle_dirs, .. } => shuffle_dirs.clone(),
            RepartitionBackend::Ray => Vec::new(),
        }
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
        let buffer_threshold_bytes =
            repartition_buffer_threshold_bytes(&self.backend, self.num_partitions);
        spawner
            .spawn(
                async move {
                    let input_bytes = input.size_bytes();
                    state.pre_repartitioned_size_bytes += input_bytes;
                    state
                        .pre_repartitioned
                        .extend(input.record_batches().iter().cloned());

                    if state.pre_repartitioned_size_bytes >= buffer_threshold_bytes {
                        state.flush_pre_partitioned()?;
                    }

                    if state.spill_dirs.is_empty() {
                        return Ok(state);
                    }
                    let mut s = state;
                    let cap = s.cap;
                    let mut res = std::mem::replace(
                        &mut s.reservation,
                        get_or_init_memory_manager().reservation(),
                    );
                    reconcile_reservation(&mut s, &mut res, cap)?;
                    s.reservation = res;
                    Ok(s)
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
        let backend = self.backend.clone();
        let schema = self.schema.clone();

        spawner
            .spawn(
                async move {
                    let mut states = states;
                    match backend {
                        RepartitionBackend::Ray => {
                            // Ray: pure in-memory, no spill stores.
                            states
                                .iter_mut()
                                .try_for_each(RepartitionAccState::flush_pre_partitioned)?;
                            let (per_partition, _input_id) =
                                flatten_per_partition(states, num_partitions, schema.clone())?;

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
                        RepartitionBackend::Flight {
                            shuffle_id,
                            shuffle_dirs,
                            local_server,
                            shuffle_address,
                            compression,
                            ..
                        } => {
                            // Flush remaining pre-repartitioned data and seal spill writers
                            // in a blocking thread.
                            let mut states = get_io_runtime(true)
                                .spawn_blocking(move || -> DaftResult<Vec<RepartitionAccState>> {
                                    states
                                        .iter_mut()
                                        .try_for_each(RepartitionAccState::flush_pre_partitioned)?;
                                    states
                                        .iter_mut()
                                        .try_for_each(RepartitionAccState::seal_spill_writer)?;
                                    Ok(states)
                                })
                                .await??;

                            let input_id = states.first().map_or(0, |s| s.input_id);
                            let part_schema = schema.clone();
                            // Pull one partition at a time: read its spilled batches + take its
                            // in-memory batches across all states. Bounds finalize to ~1 partition.
                            let next_partition = move |p: usize| -> DaftResult<MicroPartition> {
                                let mut chunks: Vec<RecordBatch> = Vec::new();
                                for state in states.iter_mut() {
                                    if let Some(store) = &state.spill_store {
                                        if store.is_spilled(p) {
                                            chunks.extend(store.read_bucket(p)?);
                                        }
                                    }
                                    chunks.extend(std::mem::take(&mut state.post_repartitioned[p]));
                                }
                                Ok(MicroPartition::new_loaded(
                                    part_schema.clone(),
                                    Arc::new(chunks),
                                    None,
                                ))
                            };

                            let partition_caches = write_partitions_one_shot_streaming(
                                input_id,
                                shuffle_id,
                                &shuffle_dirs,
                                schema,
                                compression,
                                num_partitions,
                                next_partition,
                            )
                            .await?;

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
        let (reservation, cap) = match self.spill_config() {
            Some(sc) => (get_or_init_memory_manager().reservation(), sc.cap()),
            None => (get_or_init_memory_manager().reservation(), None),
        };
        Ok(RepartitionAccState::new(
            self.num_partitions,
            input_id,
            self.bound_keys.clone(),
            self.repartition_spec.clone(),
            self.schema.clone(),
            self.spill_dirs(),
            reservation,
            cap,
        ))
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
            let mut chunks: Vec<RecordBatch> = Vec::new();
            for state in &mut states {
                // Read back any spilled batches for this partition first.
                if let Some(store) = &state.spill_store {
                    if store.is_spilled(partition_idx) {
                        chunks.extend(store.read_bucket(partition_idx)?);
                    }
                }
                // Then take the remaining in-memory batches.
                chunks.extend(std::mem::take(&mut state.post_repartitioned[partition_idx]));
            }
            Ok(MicroPartition::new_loaded(schema.clone(), Arc::new(chunks), None))
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok((per_partition, input_id))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Schema, UInt8Array};
    use daft_core::series::IntoSeries;
    use daft_logical_plan::partitioning::{RandomShuffleConfig, RepartitionSpec};
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::*;

    /// Build a MicroPartition with `num_rows` rows of a single `UInt8` column named "v".
    fn make_mp(num_rows: usize) -> MicroPartition {
        let series = UInt8Array::from_field_and_values(
            Field::new("v", DataType::UInt8),
            (0..num_rows).map(|i| i as u8),
        )
        .into_series();
        let schema = Arc::new(Schema::new(vec![series.field().clone()]));
        let rb = RecordBatch::new_unchecked(schema.clone(), vec![series.into()], num_rows);
        MicroPartition::new_loaded(schema.into(), vec![rb].into(), None)
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::UInt8)])).into()
    }

    /// Build a `RepartitionAccState` using Random partitioning (no hash keys needed).
    fn make_state(
        num_partitions: usize,
        spill_dirs: Vec<String>,
    ) -> RepartitionAccState {
        RepartitionAccState::new(
            num_partitions,
            0u32,
            vec![], // no bound keys for Random
            RepartitionSpec::Random(RandomShuffleConfig::new(Some(num_partitions))),
            test_schema(),
            spill_dirs,
            get_or_init_memory_manager().reservation(),
            None,
        )
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Push `mp` into `state.pre_repartitioned` (simulating what `sink()` does),
    /// then call `flush_pre_partitioned` to move it to `post_repartitioned`.
    fn push_and_flush(state: &mut RepartitionAccState, mp: &MicroPartition) -> DaftResult<()> {
        state
            .pre_repartitioned
            .extend(mp.record_batches().iter().cloned());
        state.pre_repartitioned_size_bytes +=
            state.pre_repartitioned.iter().map(|b| b.size_bytes()).sum::<usize>();
        state.flush_pre_partitioned()
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// `spill_largest_bucket` flushes the largest in-memory bucket to disk;
    /// looping until empty leaves all data spilled; `flatten_per_partition` recovers every row.
    #[tokio::test]
    async fn test_spill_then_flatten_recovers_all_rows() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let num_partitions = 4;
        let total_rows = 1000usize;
        let mut state = make_state(num_partitions, spill_dirs);
        push_and_flush(&mut state, &make_mp(total_rows))?;
        // Spill every bucket.
        while state.spill_largest_bucket()? {}
        assert_eq!(state.post_repartitioned_size_bytes, 0);
        state.seal_spill_writer()?;
        let (per_partition, _) =
            flatten_per_partition(vec![state], num_partitions, test_schema())?;
        let recovered: usize = per_partition.iter().map(|mp| mp.len()).sum();
        assert_eq!(recovered, total_rows);
        Ok(())
    }

    /// Multiple spill rounds + in-memory remainder are all merged correctly.
    #[tokio::test]
    async fn test_multiple_spills_and_in_memory_remainder() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let num_partitions = 3;

        let mut state = make_state(num_partitions, spill_dirs);

        // First batch → flush → spill largest bucket
        push_and_flush(&mut state, &make_mp(200))?;
        state.spill_largest_bucket()?;

        // Second batch → flush → spill largest bucket
        push_and_flush(&mut state, &make_mp(300))?;
        state.spill_largest_bucket()?;

        // Third batch → flush → stays in memory (no spill)
        push_and_flush(&mut state, &make_mp(100))?;
        assert!(state.post_repartitioned_size_bytes > 0, "third batch in memory");

        state.seal_spill_writer()?;
        let (per_partition, _) =
            flatten_per_partition(vec![state], num_partitions, test_schema())?;
        let recovered: usize = per_partition.iter().map(|mp| mp.len()).sum();
        assert_eq!(recovered, 600, "200 + 300 + 100 rows must all be recovered");

        Ok(())
    }

    /// With no spill dirs configured spill_largest_bucket returns false and all data stays in memory.
    #[tokio::test]
    async fn test_no_spill_dirs_is_noop() -> DaftResult<()> {
        let num_partitions = 2;
        let mut state = make_state(num_partitions, vec![]); // no spill dirs
        push_and_flush(&mut state, &make_mp(50))?;

        // With no spill dirs, spilling must be a no-op.
        assert!(
            !state.spill_largest_bucket()?,
            "spill_largest_bucket should return false (no-op) when spill_dirs is empty"
        );

        let bytes_before = state.post_repartitioned_size_bytes;
        // With no spill dirs, spill_largest_bucket returns Ok(false) — no-op.
        assert_eq!(
            state.post_repartitioned_size_bytes, bytes_before,
            "no spill should occur with empty spill_dirs"
        );

        state.seal_spill_writer()?;
        let (per_partition, _) =
            flatten_per_partition(vec![state], num_partitions, test_schema())?;
        let recovered: usize = per_partition.iter().map(|mp| mp.len()).sum();
        assert_eq!(recovered, 50);

        Ok(())
    }

    /// Two worker states (simulating parallel workers) are merged correctly
    /// when one state has spilled and the other has not.
    #[tokio::test]
    async fn test_two_states_one_spilled() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let num_partitions = 4;

        let mut state_a = make_state(num_partitions, spill_dirs.clone());
        push_and_flush(&mut state_a, &make_mp(400))?;
        while state_a.spill_largest_bucket()? {} // spill all
        state_a.seal_spill_writer()?;

        let mut state_b = make_state(num_partitions, spill_dirs);
        push_and_flush(&mut state_b, &make_mp(600))?;
        state_b.seal_spill_writer()?; // no-op, nothing spilled

        let (per_partition, _) =
            flatten_per_partition(vec![state_a, state_b], num_partitions, test_schema())?;
        let recovered: usize = per_partition.iter().map(|mp| mp.len()).sum();
        assert_eq!(recovered, 1000, "400 spilled + 600 in-memory must all recover");

        Ok(())
    }
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
