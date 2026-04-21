use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
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

pub(crate) struct RayGatherState {
    partitions: Vec<MicroPartition>,
}

impl RayGatherState {
    fn push(&mut self, input: MicroPartition) {
        self.partitions.push(input);
    }
}

/// Shared config for all Flight gather states produced by a single GatherSink.
struct FlightShared {
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    local_server: Arc<ShuffleFlightServer>,
    shuffle_address: String,
    target_in_memory_size_per_partition: usize,
    next_partition_ref_id: AtomicU32,
}

pub(crate) struct FlightGatherState {
    shared: Arc<FlightShared>,
    input_id: InputId,
    refs: Vec<FlightPartitionRef>,
}

impl FlightGatherState {
    async fn push(&mut self, input: MicroPartition) -> DaftResult<()> {
        let shared = &self.shared;
        let idx = shared.next_partition_ref_id.fetch_add(1, Ordering::Relaxed);
        let partition_ref_id = partition_ref_id(self.input_id, idx as usize);
        let cache = InProgressShuffleCache::try_new(
            partition_ref_id,
            &shared.shuffle_dirs,
            shared.shuffle_id,
            shared.target_in_memory_size_per_partition,
            shared.compression.as_deref(),
        )?;
        cache.push_partition_data(input).await?;
        let closed = cache.close().await?;
        let flight_ref = FlightPartitionRef {
            shuffle_id: shared.shuffle_id,
            server_address: shared.shuffle_address.clone(),
            partition_ref_id: closed.partition_ref_id,
            num_rows: closed.num_rows,
            size_bytes: closed.size_bytes,
        };
        shared
            .local_server
            .register_shuffle_partitions(shared.shuffle_id, vec![closed])
            .await?;
        self.refs.push(flight_ref);
        Ok(())
    }
}

pub(crate) enum GatherState {
    Ray(RayGatherState),
    Flight(FlightGatherState),
}

impl GatherState {
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

// TODO: unify shuffle backends in all local operations
#[derive(Clone)]
enum GatherBackend {
    Ray,
    Flight(Arc<FlightShared>),
}

impl GatherBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::Ray => "Ray",
            Self::Flight(_) => "Flight",
        }
    }

    fn collect_output(&self, states: Vec<GatherState>) -> BlockingSinkOutput {
        match self {
            Self::Ray => BlockingSinkOutput::Partitions(
                states
                    .into_iter()
                    .flat_map(|s| match s {
                        GatherState::Ray(s) => s.partitions,
                        GatherState::Flight(_) => unreachable!("GatherSink state/backend mismatch"),
                    })
                    .collect(),
            ),
            Self::Flight(_) => BlockingSinkOutput::FlightPartitionRefs(
                states
                    .into_iter()
                    .flat_map(|s| match s {
                        GatherState::Flight(s) => s.refs,
                        GatherState::Ray(_) => unreachable!("GatherSink state/backend mismatch"),
                    })
                    .collect(),
            ),
        }
    }
}

pub struct GatherSink {
    backend: GatherBackend,
}

impl GatherSink {
    pub fn new_ray() -> Self {
        Self {
            backend: GatherBackend::Ray,
        }
    }

    pub fn try_new_flight(
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
    ) -> DaftResult<Self> {
        const TARGET_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 64;
        Ok(Self {
            backend: GatherBackend::Flight(Arc::new(FlightShared {
                shuffle_id,
                shuffle_dirs,
                compression,
                local_server,
                shuffle_address,
                target_in_memory_size_per_partition: TARGET_IN_MEMORY_SIZE_BYTES,
                next_partition_ref_id: AtomicU32::new(0),
            })),
        })
    }
}

impl BlockingSink for GatherSink {
    type State = GatherState;

    #[instrument(skip_all, name = "GatherSink::sink")]
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

    #[instrument(skip_all, name = "GatherSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let backend = self.backend.clone();
        spawner
            .spawn(
                async move { Ok(backend.collect_output(states)) },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("Gather({})", self.backend.name()).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("Gather({})", self.backend.name())]
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            GatherBackend::Ray => Ok(GatherState::Ray(RayGatherState {
                partitions: Vec::new(),
            })),
            GatherBackend::Flight(shared) => Ok(GatherState::Flight(FlightGatherState {
                shared: shared.clone(),
                input_id,
                refs: Vec::new(),
            })),
        }
    }
}
