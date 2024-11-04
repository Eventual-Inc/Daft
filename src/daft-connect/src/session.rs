use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

pub struct Session {
    /// so order is preserved, and so we can efficiently do a prefix search
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    #[expect(
        unused,
        reason = "this will be used in the future especially to pass spark connect tests"
    )]
    tables_by_name: HashMap<String, daft_table::Table>,

    id: String,
    server_side_session_id: String,
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
        Self {
            config_values: Default::default(),
            tables_by_name: Default::default(),
            id,
            server_side_session_id,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn server_side_session_id(&self) -> &str {
        &self.server_side_session_id
    }
}
