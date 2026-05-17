use std::sync::{Arc, OnceLock};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_recordbatch::RecordBatch;
use daft_shuffles::{
    oneshot_writer::write_partitions_one_shot, server::flight_server::ShuffleFlightServer,
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

const REPARTITION_BUFFER_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Per-(worker, input) accumulator. Morsels are buffered until they cross
/// `REPARTITION_BUFFER_THRESHOLD_BYTES`, then fused and partitioned in one pass.
pub(crate) struct RepartitionAccState {
    per_partition: Vec<Vec<MicroPartition>>,
    buffered: Vec<MicroPartition>,
    buffered_bytes: usize,
    input_id: InputId,
}

impl RepartitionAccState {
    fn new(num_partitions: usize, input_id: InputId) -> Self {
        Self {
            per_partition: (0..num_partitions).map(|_| Vec::new()).collect(),
            buffered: Vec::new(),
            buffered_bytes: 0,
            input_id,
        }
    }
}

/// Fuse buffered morsels, partition once, and append to `state.per_partition`.
fn flush_buffer(
    state: &mut RepartitionAccState,
    repartition_spec: &RepartitionSpec,
    bound_keys: &OnceLock<Arc<Vec<BoundExpr>>>,
    num_partitions: usize,
) -> DaftResult<()> {
    if state.buffered.is_empty() {
        return Ok(());
    }
    let buffered = std::mem::take(&mut state.buffered);
    state.buffered_bytes = 0;

    let schema = buffered[0].schema();
    let mut all_chunks: Vec<RecordBatch> = Vec::new();
    for mp in &buffered {
        all_chunks.extend(mp.record_batches().iter().cloned());
    }
    if all_chunks.is_empty() {
        return Ok(());
    }
    let fused_batch = RecordBatch::concat(&all_chunks)?;
    let fused = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![fused_batch]), None);

    let partitioned = match repartition_spec {
        RepartitionSpec::Hash(config) => {
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
    debug_assert_eq!(partitioned.len(), state.per_partition.len());
    for (acc, part) in state.per_partition.iter_mut().zip(partitioned) {
        acc.push(part);
    }
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
    bound_keys: Arc<OnceLock<Arc<Vec<BoundExpr>>>>,
    /// Bytes-per-row estimate so we can skip the per-morsel `size_bytes()` walk.
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
                compression: parse_compression(compression.as_deref())?,
            },
            repartition_spec,
            num_partitions,
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
            let _ = self.bytes_per_row.set(observed as f64 / input.len() as f64);
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
                    state.buffered_bytes += input_bytes;
                    state.buffered.push(input);

                    if state.buffered_bytes >= REPARTITION_BUFFER_THRESHOLD_BYTES {
                        flush_buffer(&mut state, &repartition_spec, &bound_keys, num_partitions)?;
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
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let repartition_spec = self.repartition_spec.clone();
        let bound_keys = self.bound_keys.clone();

        let flush_result = states
            .iter_mut()
            .try_for_each(|s| flush_buffer(s, &repartition_spec, &bound_keys, num_partitions));
        if let Err(e) = flush_result {
            return spawner.spawn(async move { Err(e) }, Span::current()).into();
        }

        let (per_partition, input_id) = flatten_per_partition(states, num_partitions);

        match &self.backend {
            RepartitionBackend::Ray => spawner
                .spawn(
                    async move {
                        let mut outputs = Vec::with_capacity(num_partitions);
                        for data in per_partition {
                            let fut = tokio::spawn(async move {
                                if data.is_empty() {
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
                                Ok::<_, DaftError>(mp)
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
                .into(),
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
                            let partition_caches = write_partitions_one_shot(
                                input_id,
                                shuffle_id,
                                &shuffle_dirs,
                                schema,
                                compression,
                                per_partition,
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

fn flatten_per_partition(
    states: Vec<RepartitionAccState>,
    num_partitions: usize,
) -> (Vec<Vec<MicroPartition>>, InputId) {
    let input_id = states
        .first()
        .map(|s| s.input_id)
        .expect("RepartitionSink::finalize called with no states");
    debug_assert!(states.iter().all(|s| s.input_id == input_id));

    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    for state in states {
        for (i, mut chunk) in state.per_partition.into_iter().enumerate() {
            per_partition[i].append(&mut chunk);
        }
    }
    (per_partition, input_id)
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
