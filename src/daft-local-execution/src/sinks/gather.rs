use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::shuffle_cache::{InProgressShuffleCache, partition_ref_id};
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
    },
    shuffle_backend::{FlightShuffleContext, LocalShuffleBackend},
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

// Each input MP gets its own cache (unlike repartition, which keeps N open caches per
// input and divides a global budget by `num_partitions`). Matches the upper end of
// `RepartitionSink`'s per-partition clamp so the two sinks spill at similar sizes.
const TARGET_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 128;

pub(crate) struct RayGatherState {
    partitions: Vec<MicroPartition>,
}

impl RayGatherState {
    fn push(&mut self, input: MicroPartition) {
        self.partitions.push(input);
    }
}

pub(crate) struct FlightGatherState {
    shared: Arc<FlightShuffleContext>,
    schema: SchemaRef,
    input_id: InputId,
    refs: Vec<FlightPartitionRef>,
}

impl FlightGatherState {
    async fn push(&mut self, input: MicroPartition) -> DaftResult<()> {
        let shared = &self.shared;
        let partition_ref_id = partition_ref_id(self.input_id, self.refs.len());
        let cache = InProgressShuffleCache::try_new(
            partition_ref_id,
            self.schema.clone(),
            &shared.shuffle_dirs,
            shared.shuffle_id,
            TARGET_IN_MEMORY_SIZE_BYTES,
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
        if input.is_empty() {
            return Ok(());
        }
        match self {
            Self::Ray(state) => {
                state.push(input);
                Ok(())
            }
            Self::Flight(state) => state.push(input).await,
        }
    }
}

fn collect_output(backend: &LocalShuffleBackend, states: Vec<GatherState>) -> BlockingSinkOutput {
    match backend {
        LocalShuffleBackend::Ray => BlockingSinkOutput::Partitions(
            states
                .into_iter()
                .flat_map(|s| match s {
                    GatherState::Ray(s) => s.partitions,
                    GatherState::Flight(_) => unreachable!("GatherSink state/backend mismatch"),
                })
                .collect(),
        ),
        LocalShuffleBackend::Flight(_) => BlockingSinkOutput::FlightPartitionRefs(
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

pub struct GatherSink {
    schema: SchemaRef,
    backend: LocalShuffleBackend,
}

impl GatherSink {
    pub fn new(schema: SchemaRef, backend: LocalShuffleBackend) -> Self {
        Self { schema, backend }
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
                async move { Ok(collect_output(&backend, states)) },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("Gather({})", self.backend.name()).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Gather
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("Gather({})", self.backend.name())]
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            LocalShuffleBackend::Ray => Ok(GatherState::Ray(RayGatherState {
                partitions: Vec::new(),
            })),
            LocalShuffleBackend::Flight(shared) => Ok(GatherState::Flight(FlightGatherState {
                shared: shared.clone(),
                schema: self.schema.clone(),
                input_id,
                refs: Vec::new(),
            })),
        }
    }
}
