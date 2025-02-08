mod error;

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daft_catalog::{identifier::Identifier, DaftCatalog};
use daft_logical_plan::LogicalPlanBuilder;

use crate::error::Result;

/// Temporary rename while refactoring.
type Metastore = DaftCatalog;

/// Session holds all state for query planning and execution (e.g. connection).
#[derive(Debug)]
pub struct Session {
    /// Session state for interior mutability
    state: Arc<RwLock<SessionState>>,
}

/// Session state is to be kept internal, consider a builder.
#[derive(Debug)]
struct SessionState {
    /// Session identifier
    _id: String,
    // TODO remove DaftCatalog after all APIs are migrated.
    metastore: DaftCatalog,
    // TODO execution context
    // TODO session options
}

impl Session {
    /// Creates a new empty session
    pub fn new(id: &str, metastore: Metastore) -> Self {
        let state = SessionState {
            _id: id.to_string(),
            metastore,
        };
        let state = RwLock::new(state);
        let state = Arc::new(state);
        Self { state }
    }

    /// Get an immutable reference to the state.
    fn state(&self) -> RwLockReadGuard<'_, SessionState> {
        self.state.read().unwrap()
    }

    /// Get a mutable reference to the state.
    fn state_mut(&self) -> RwLockWriteGuard<'_, SessionState> {
        self.state.write().unwrap()
    }

    /// Creates a table backed by the view
    /// TODO support names/namespaces and other table sources.
    pub fn create_table(&self, name: &str, view: impl Into<LogicalPlanBuilder>) -> Result<()> {
        self.state_mut().metastore.register_table(name, view)
    }

    /// Gets a table by ident
    pub fn get_table(&self, ident: &Identifier) -> Result<LogicalPlanBuilder> {
        // TODO current read_table accepts an unparsed string
        let table_identifier = ident.to_string();
        // if ident.has_namespace() {
        //     unsupported!("qualified table names")
        // }
        self.state().metastore.read_table(&table_identifier)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new("default", Metastore::default())
    }
}
