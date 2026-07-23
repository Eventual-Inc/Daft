use std::sync::Arc;

use daft_shuffles::server::flight_server::ShuffleFlightServer;

/// Transport handles shared by all Flight-backed states of a single shuffle sink.
///
/// Sink-specific inputs (the schema, per-partition cache spill targets) are
/// intentionally not stored here: they are owned by each sink, since not every
/// sink reads them from a shared context (e.g. repartition uses its own schema
/// and one-shot writer with no per-partition spill knob).
pub(crate) struct FlightShuffleContext {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
    pub(crate) local_server: Arc<ShuffleFlightServer>,
    pub(crate) shuffle_address: String,
}

/// Picks between the Ray path and the Flight path for local shuffle operators
/// and carries Flight's runtime handles.
#[derive(Clone)]
pub(crate) enum LocalShuffleBackend {
    Ray,
    Flight(Arc<FlightShuffleContext>),
}

impl LocalShuffleBackend {
    /// Resolve a plan-level [`daft_local_plan::ShuffleBackend`] into the runtime
    /// backend, threading in the worker's Flight server for the Flight path.
    /// This is the single point where the shuffle server is resolved for all
    /// local shuffle sinks.
    pub(crate) fn from_plan(
        backend: &daft_local_plan::ShuffleBackend,
        shuffle_server: Option<(Arc<ShuffleFlightServer>, String)>,
    ) -> Self {
        match backend {
            daft_local_plan::ShuffleBackend::Ray => Self::Ray,
            daft_local_plan::ShuffleBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                compression,
            } => {
                let (local_server, shuffle_address) = shuffle_server.expect(
                    "Flight shuffle server must be initialized for Flight shuffle plans when using flight_shuffle algorithm",
                );
                Self::Flight(Arc::new(FlightShuffleContext {
                    shuffle_id: *shuffle_id,
                    shuffle_dirs: shuffle_dirs.clone(),
                    compression: compression.clone(),
                    local_server,
                    shuffle_address,
                }))
            }
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Ray => "Ray",
            Self::Flight(_) => "Flight",
        }
    }
}
