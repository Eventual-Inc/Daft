use std::sync::Arc;

use daft_core::prelude::SchemaRef;
use daft_shuffles::server::flight_server::ShuffleFlightServer;

/// Runtime config shared by all Flight-backed states of a single shuffle sink.
pub(crate) struct FlightShuffleContext {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
    pub(crate) local_server: Arc<ShuffleFlightServer>,
    pub(crate) shuffle_address: String,
    pub(crate) target_in_memory_size_per_partition: usize,
    pub(crate) schema: SchemaRef,
}

/// Picks between the Ray path and the Flight path for local shuffle operators
/// and carries Flight's runtime handles.
// TODO: unify shuffle backends in all local operations. Remaining:
// `sinks/repartition.rs` and the dispatch blocks in `pipeline.rs`.
#[derive(Clone)]
pub(crate) enum LocalShuffleBackend {
    Ray,
    Flight(Arc<FlightShuffleContext>),
}

impl LocalShuffleBackend {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Ray => "Ray",
            Self::Flight(_) => "Flight",
        }
    }
}
