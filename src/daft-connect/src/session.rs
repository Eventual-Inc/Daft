use std::collections::BTreeMap;

use common_runtime::RuntimeRef;
use daft_context::DaftContext;
use uuid::Uuid;

#[derive(Clone)]
pub struct Session {
    ctx: DaftContext,
    /// so order is preserved, and so we can efficiently do a prefix search
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    id: String,
    server_side_session_id: String,
    pub(crate) compute_runtime: RuntimeRef,
}

impl Session {
    pub fn ctx(&self) -> &DaftContext {
        &self.ctx
    }

    pub fn ctx_mut(&mut self) -> &mut DaftContext {
        &mut self.ctx
    }

    pub fn config_values(&self) -> &BTreeMap<String, String> {
        &self.config_values
    }

    pub fn config_values_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.config_values
    }

    pub fn new(id: String) -> Self {
        let server_side_session_id = Uuid::new_v4();
        let server_side_session_id = server_side_session_id.to_string();
        let ctx = DaftContext::new();
        let compute_runtime = common_runtime::get_compute_runtime();

        Self {
            config_values: Default::default(),
            id,
            server_side_session_id,
            ctx,
            compute_runtime,
        }
    }

    pub fn client_side_session_id(&self) -> &str {
        &self.id
    }

    pub fn server_side_session_id(&self) -> &str {
        &self.server_side_session_id
    }
}
