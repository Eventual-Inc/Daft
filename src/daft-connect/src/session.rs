use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use common_runtime::RuntimeRef;
use daft_session::Session;
use uuid::Uuid;

#[derive(Clone)]
pub struct ConnectSession {
    /// so order is preserved, and so we can efficiently do a prefix search
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    id: String,
    server_side_session_id: String,
    pub(crate) compute_runtime: RuntimeRef,
    pub session: Arc<RwLock<Session>>,
}

impl ConnectSession {
    pub fn config_values(&self) -> &BTreeMap<String, String> {
        &self.config_values
    }

    pub fn config_values_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.config_values
    }

    pub fn new(id: String) -> Self {
        let server_side_session_id = Uuid::new_v4();
        let server_side_session_id = server_side_session_id.to_string();
        let compute_runtime = common_runtime::get_compute_runtime();
        let session = Arc::new(RwLock::new(Session::default()));

        Self {
            config_values: Default::default(),
            id,
            server_side_session_id,
            compute_runtime,
            session,
        }
    }

    pub fn client_side_session_id(&self) -> &str {
        &self.id
    }

    pub fn server_side_session_id(&self) -> &str {
        &self.server_side_session_id
    }

    /// get a read only reference to the session
    pub fn session(&self) -> RwLockReadGuard<'_, Session> {
        self.session.read().expect("catalog lock poisoned")
    }

    /// get a mutable reference to the session
    pub fn session_mut(&self) -> std::sync::RwLockWriteGuard<'_, Session> {
        self.session.write().expect("catalog lock poisoned")
    }
}
