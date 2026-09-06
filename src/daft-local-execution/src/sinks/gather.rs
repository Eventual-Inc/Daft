use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::{
    shuffle_cache::{InProgressShuffleCache, partition_ref_id},
    store::new_attempt_token,
};
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

/// The numbering every state of one input draws from.
///
/// Refs from all of an input's states are flattened into a single output list
/// (`collect_output`), one entry per gathered partition, so each needs its own
/// `partition_ref_id`. Two states numbering their own outputs from zero would
/// give two different partitions the same id — which used to lose rows outright,
/// because the second registration overwrote the first, and is otherwise only
/// masked by whatever else happens to be in the registry key.
///
/// The attempt token is shared for the same reason, in the other direction: it
/// identifies one *execution of the map task*, as it does everywhere else, and
/// handing each state its own would quietly redefine it as "one worker's share of
/// an execution".
struct InputOutputIds {
    attempt: u64,
    next_idx: AtomicUsize,
}

impl InputOutputIds {
    fn new() -> Self {
        Self {
            attempt: new_attempt_token(),
            next_idx: AtomicUsize::new(0),
        }
    }
}

pub(crate) struct FlightGatherState {
    shared: Arc<FlightShuffleContext>,
    schema: SchemaRef,
    input_id: InputId,
    ids: Arc<InputOutputIds>,
    refs: Vec<FlightPartitionRef>,
}

impl FlightGatherState {
    async fn push(&mut self, input: MicroPartition) -> DaftResult<()> {
        let shared = &self.shared;
        let partition_ref_id = partition_ref_id(
            self.input_id,
            self.ids.next_idx.fetch_add(1, Ordering::Relaxed),
        );
        let cache = InProgressShuffleCache::try_new(
            partition_ref_id,
            self.ids.attempt,
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
            attempt: self.ids.attempt,
            num_rows: closed.num_rows,
            size_bytes: closed.size_bytes,
        };
        shared.local_server.register_shuffle_partitions(
            shared.shuffle_id,
            self.ids.attempt,
            vec![closed],
        )?;
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
    /// One [`InputOutputIds`] per input, shared by every worker's state for it.
    /// Entries are dropped at that input's finalize.
    ids_by_input: Mutex<HashMap<InputId, Arc<InputOutputIds>>>,
}

impl GatherSink {
    pub fn new(schema: SchemaRef, backend: LocalShuffleBackend) -> Self {
        Self {
            schema,
            backend,
            ids_by_input: Mutex::new(HashMap::new()),
        }
    }

    fn ids_for(&self, input_id: InputId) -> Arc<InputOutputIds> {
        self.ids_by_input
            .lock()
            .expect("gather id table poisoned")
            .entry(input_id)
            .or_insert_with(|| Arc::new(InputOutputIds::new()))
            .clone()
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
        // This input is done producing refs, so its numbering can go. Without
        // this the table grows by one entry per input for the node's lifetime.
        if let Some(GatherState::Flight(state)) = states.first() {
            self.ids_by_input
                .lock()
                .expect("gather id table poisoned")
                .remove(&state.input_id);
        }
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
                ids: self.ids_for(input_id),
                refs: Vec::new(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the old numbering broke: every state of one input draws from
    /// the same sequence, so no two gathered partitions can claim the same id.
    #[test]
    fn concurrent_states_of_one_input_never_share_a_ref_id() {
        let ids = Arc::new(InputOutputIds::new());
        let input_id: InputId = 9;

        // Two workers' states, interleaved the way the scheduler would run them.
        let (a, b) = (ids.clone(), ids);
        let mut seen = Vec::new();
        for _ in 0..4 {
            for state in [&a, &b] {
                seen.push(partition_ref_id(
                    input_id,
                    state.next_idx.fetch_add(1, Ordering::Relaxed),
                ));
            }
        }

        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), seen.len(), "ref ids collided: {seen:?}");
        // And they all still decode back to this input.
        assert!(seen.iter().all(|id| (id >> 32) as u32 == input_id));
    }

    /// The attempt token means "one execution of this map task" everywhere else in
    /// the shuffle; gather must not redefine it as "one worker's share".
    #[test]
    fn every_state_of_one_input_shares_one_attempt() {
        let backend = LocalShuffleBackend::Ray;
        let sink = GatherSink::new(Arc::new(daft_core::prelude::Schema::empty()), backend);
        let first = sink.ids_for(3);
        let second = sink.ids_for(3);
        let other_input = sink.ids_for(4);

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.attempt, second.attempt);
        assert_ne!(first.attempt, other_input.attempt);
    }
}
