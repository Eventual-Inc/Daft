use std::sync::Arc;

use daft_common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::{
    server::flight_server::ShuffleFlightServer,
    shuffle_cache::{InProgressShuffleCache, partition_ref_id},
};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

pub(crate) struct RayIntoPartitionsState {
    partitions: Vec<MicroPartition>,
}

impl RayIntoPartitionsState {
    fn push(&mut self, part: MicroPartition) {
        self.partitions.push(part);
    }

    fn finalize(
        states: Vec<Self>,
        num_partitions: usize,
        schema: SchemaRef,
    ) -> DaftResult<Vec<MicroPartition>> {
        let all_parts: Vec<_> = states.into_iter().flat_map(|s| s.partitions).collect();
        let concatenated = MicroPartition::concat(all_parts)?;

        let total_rows = concatenated.len();
        let rows_per_partition = total_rows.div_ceil(num_partitions);

        let mut outputs = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            let start_idx = i * rows_per_partition;
            let end_idx = std::cmp::min(start_idx + rows_per_partition, total_rows);

            if start_idx < total_rows {
                outputs.push(concatenated.slice(start_idx, end_idx)?);
            } else {
                outputs.push(MicroPartition::empty(Some(schema.clone())));
            }
        }
        Ok(outputs)
    }
}

pub(crate) struct FlightIntoPartitionsState {
    shared: Arc<FlightShared>,
    caches: Vec<InProgressShuffleCache>,
    rotation_offset: usize,
}

impl FlightIntoPartitionsState {
    fn try_new(
        shared: Arc<FlightShared>,
        input_id: InputId,
        num_partitions: usize,
    ) -> DaftResult<Self> {
        let mut caches = Vec::with_capacity(num_partitions);
        for partition_idx in 0..num_partitions {
            let partition_ref_id = partition_ref_id(input_id, partition_idx);
            let cache = InProgressShuffleCache::try_new(
                partition_ref_id,
                shared.schema.clone(),
                &shared.shuffle_dirs,
                shared.shuffle_id,
                shared.target_in_memory_size_per_partition,
                shared.compression.as_deref(),
            )?;
            caches.push(cache);
        }
        Ok(Self {
            shared,
            caches,
            rotation_offset: 0,
        })
    }

    /// Slice the input into N row-chunks and append each to its cache,
    /// rotating the bucket offset per call so that `div_ceil` front-loading
    /// doesn't consistently favour early buckets.
    async fn push(&mut self, input: MicroPartition) -> DaftResult<()> {
        let total = input.len();
        if total == 0 {
            return Ok(());
        }
        let n = self.caches.len();
        let rows_per_bucket = total.div_ceil(n);

        for i in 0..n {
            let start = i * rows_per_bucket;
            if start >= total {
                break;
            }
            let end = (start + rows_per_bucket).min(total);
            let slice = input.slice(start, end)?;
            let bucket = (i + self.rotation_offset) % n;
            self.caches[bucket].push_partition_data(slice).await?;
        }
        self.rotation_offset = (self.rotation_offset + 1) % n;
        Ok(())
    }

    async fn finalize(self) -> DaftResult<Vec<FlightPartitionRef>> {
        let Self { shared, caches, .. } = self;

        let closed_list = futures::future::try_join_all(
            caches.into_iter().map(|c| async move { c.close().await }),
        )
        .await?;

        let refs: Vec<_> = closed_list
            .iter()
            .map(|closed| FlightPartitionRef {
                shuffle_id: shared.shuffle_id,
                server_address: shared.shuffle_address.clone(),
                partition_ref_id: closed.partition_ref_id,
                num_rows: closed.num_rows,
                size_bytes: closed.size_bytes,
            })
            .collect();

        shared
            .local_server
            .register_shuffle_partitions(shared.shuffle_id, closed_list)
            .await?;

        Ok(refs)
    }
}

pub(crate) enum IntoPartitionsState {
    Ray(RayIntoPartitionsState),
    Flight(FlightIntoPartitionsState),
}

impl IntoPartitionsState {
    async fn push(&mut self, input: MicroPartition) -> DaftResult<()> {
        match self {
            Self::Ray(state) => {
                state.push(input);
                Ok(())
            }
            Self::Flight(state) => state.push(input).await,
        }
    }
}

/// Shared Flight config for all states produced by one IntoPartitionsSink.
struct FlightShared {
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    local_server: Arc<ShuffleFlightServer>,
    shuffle_address: String,
    target_in_memory_size_per_partition: usize,
    schema: SchemaRef,
}

// Mirrors the pattern in `sinks/gather.rs::GatherBackend` and
// `sinks/repartition.rs::RepartitionBackend` — a runtime enum that picks
// between the Ray path and the Flight path and carries Flight's runtime handles.
#[derive(Clone)]
enum IntoPartitionsBackend {
    Ray { schema: SchemaRef },
    Flight(Arc<FlightShared>),
}

impl IntoPartitionsBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Ray { .. } => "Ray",
            Self::Flight(_) => "Flight",
        }
    }
}

pub struct IntoPartitionsSink {
    num_partitions: usize,
    backend: IntoPartitionsBackend,
}

impl IntoPartitionsSink {
    pub fn new_ray(num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            num_partitions,
            backend: IntoPartitionsBackend::Ray { schema },
        }
    }

    pub fn try_new_flight(
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
    ) -> DaftResult<Self> {
        // Mirror `RepartitionSink::try_new_flight`: divide a global 2000 MiB budget
        // evenly across the output partitions, clamped to [8 MiB, 128 MiB] per
        // cache so we spill at roughly the same rate regardless of N.
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000;
        let target_in_memory_size_per_partition = (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
            / num_partitions.max(1))
        .clamp(1024 * 1024 * 8, 1024 * 1024 * 128);
        Ok(Self {
            num_partitions,
            backend: IntoPartitionsBackend::Flight(Arc::new(FlightShared {
                shuffle_id,
                shuffle_dirs,
                compression,
                local_server,
                shuffle_address,
                target_in_memory_size_per_partition,
                schema,
            })),
        })
    }
}

impl BlockingSink for IntoPartitionsSink {
    type State = IntoPartitionsState;

    #[instrument(skip_all, name = "IntoPartitionsSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        spawner
            .spawn(
                async move {
                    state.push(input).await?;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "IntoPartitionsSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let backend = self.backend.clone();
        let num_partitions = self.num_partitions;

        spawner
            .spawn(
                async move {
                    match backend {
                        IntoPartitionsBackend::Ray { schema } => {
                            let ray_states = states
                                .into_iter()
                                .map(|s| match s {
                                    IntoPartitionsState::Ray(s) => s,
                                    _ => unreachable!("IntoPartitionsSink state/backend mismatch"),
                                })
                                .collect();
                            let outputs = RayIntoPartitionsState::finalize(
                                ray_states,
                                num_partitions,
                                schema,
                            )?;
                            Ok(BlockingSinkOutput::Partitions(outputs))
                        }
                        IntoPartitionsBackend::Flight(_) => {
                            debug_assert_eq!(
                                states.len(),
                                1,
                                "IntoPartitionsSink Flight expects a single state (max_concurrency == 1)"
                            );
                            let flight_state = states
                                .into_iter()
                                .map(|s| match s {
                                    IntoPartitionsState::Flight(s) => s,
                                    _ => unreachable!("IntoPartitionsSink state/backend mismatch"),
                                })
                                .next()
                                .expect("IntoPartitionsSink Flight backend got no state");
                            let refs = flight_state.finalize().await?;
                            Ok(BlockingSinkOutput::FlightPartitionRefs(refs))
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("IntoPartitions({})", self.backend.name()).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::IntoPartitions
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "IntoPartitions({}): Into {} partitions",
            self.backend.name(),
            self.num_partitions
        )]
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            IntoPartitionsBackend::Ray { .. } => {
                Ok(IntoPartitionsState::Ray(RayIntoPartitionsState {
                    partitions: Vec::new(),
                }))
            }
            IntoPartitionsBackend::Flight(shared) => Ok(IntoPartitionsState::Flight(
                FlightIntoPartitionsState::try_new(shared.clone(), input_id, self.num_partitions)?,
            )),
        }
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
