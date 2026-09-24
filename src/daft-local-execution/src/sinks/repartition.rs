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
use daft_shuffles::{oneshot_writer::write_partitions_one_shot, shuffle_cache::CHUNK_TARGET_BYTES};
use itertools::Itertools;
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
    },
    shuffle_backend::LocalShuffleBackend,
};
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

fn repartition_buffer_threshold_bytes(
    backend: &LocalShuffleBackend,
    num_partitions: usize,
) -> usize {
    match backend {
        LocalShuffleBackend::Ray => REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
        LocalShuffleBackend::Flight(_) => CHUNK_TARGET_BYTES
            .saturating_mul(num_partitions.max(1))
            .clamp(
                REPARTITION_MIN_BUFFER_THRESHOLD_BYTES,
                REPARTITION_MAX_BUFFER_THRESHOLD_BYTES,
            ),
    }
}

pub struct RepartitionSink {
    backend: LocalShuffleBackend,
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
        let backend = self.backend.clone();
        let schema = self.schema.clone();

        spawner
            .spawn(
                async move {
                    let mut states = states;
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
        Ok(RepartitionAccState::new(
            self.num_partitions,
            input_id,
            self.bound_keys.clone(),
            self.repartition_spec.clone(),
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
