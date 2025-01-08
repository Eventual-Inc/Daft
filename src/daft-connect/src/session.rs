use std::collections::BTreeMap;

use common_runtime::{get_compute_runtime, RuntimeRef};
use daft_local_execution::NativeExecutor;
use daft_micropartition::partitioning::InMemoryPartitionSetCache;
use daft_ray_execution::RayRunnerShim;
use uuid::Uuid;

use crate::runner::Runner;

pub struct Session {
    /// so order is preserved, and so we can efficiently do a prefix search
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    id: String,
    server_side_session_id: String,
    /// MicroPartitionSet associated with this session
    /// this will be filled up as the user runs queries
    pub(crate) psets: InMemoryPartitionSetCache,
    pub runtime: RuntimeRef,
}

impl Session {
    pub fn config_values(&self) -> &BTreeMap<String, String> {
        &self.config_values
    }

    pub fn config_values_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.config_values
    }

    pub fn new(id: String) -> Self {
        let server_side_session_id = Uuid::new_v4();
        let server_side_session_id = server_side_session_id.to_string();
        let rt = get_compute_runtime();
        Self {
            config_values: Default::default(),
            id,
            server_side_session_id,
            psets: InMemoryPartitionSetCache::empty(),
            runtime: rt,
        }
    }

    pub fn client_side_session_id(&self) -> &str {
        &self.id
    }

    pub fn server_side_session_id(&self) -> &str {
        &self.server_side_session_id
    }
}
