use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use daft_catalog::{Catalogs, Catalog, Identifier};
use daft_logical_plan::LogicalPlanBuilder;
use uuid::Uuid;

// use options::Options;
use crate::{
    error::Result, obj_already_exists_err, obj_not_found_err, options::Options, unsupported_err,
};

/// Session holds all state for query planning and execution (e.g. connection).
#[derive(Debug, Clone)]
pub struct Session {
    /// Session state for interior mutability
    state: Arc<RwLock<SessionState>>,
}

/// Session state is to be kept internal, consider a builder.
#[derive(Debug)]
struct SessionState {
    /// Session identifier
    _id: String,
    /// Session options i.e. curr_catalog and curr_schema.
    options: Options,
    /// References to the available catalogs.
    catalogs: Catalogs,
    // TODO execution context
    // TODO temporary! tables come from a catalog, not here!!
    tables: HashMap<String, LogicalPlanBuilder>,
    // TODO identifier matcher for case-insensitive matching
}

impl Session {
    /// Creates a new empty session
    pub fn empty() -> Self {
        let state = SessionState {
            _id: Uuid::new_v4().to_string(),
            options: Options::default(),
            catalogs: Catalogs::empty(),
            tables: HashMap::new(),
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

    /// Attaches a catalog to this session, err if already exists.
    pub fn attach(&self, name: String, catalog: Arc<dyn Catalog>) -> Result<()> {
        if self.state().catalogs.exists(&name) {
            obj_already_exists_err!("Catalog", &name.into())
        }
        self.state_mut().catalogs.attach(name, catalog);
        Ok(())
    }

    /// Detaches a catalog from this session, err if does not exist.
    pub fn detach(&self, name: &str) -> Result<()> {
        if self.state().catalogs.exists(name) {
            obj_not_found_err!("Catalog", &name.into())
        }
        self.state_mut().catalogs.detach(name);
        Ok(())
    }

    /// Creates a table backed by the view
    /// TODO support other table sources.
    pub fn create_table(
        &self,
        name: Identifier,
        view: impl Into<LogicalPlanBuilder>,
    ) -> Result<()> {
        if name.has_namespace() {
            unsupported_err!("Creating a table with a namespace is not yet supported, Instead use a single identifier, or wrap your table name in quotes such as `\"{}\"`", name);
        }
        self.state_mut().tables.insert(name.name, view.into());
        Ok(())
    }

    /// Returns the session's current catalog.
    pub fn current_catalog(&self) -> Result<Arc<dyn Catalog>> {
        todo!()
    }

    /// Returns the session's current schema.
    pub fn current_namespace(&self) -> Result<()> {
        todo!()
    }

    /// Gets a table by identifier
    pub fn get_table(&self, name: &Identifier) -> Result<LogicalPlanBuilder> {
        if name.has_namespace() {
            unsupported_err!("Qualified identifiers are not yet supported")
        }
        if let Some(view) = self.state().tables.get(&name.name) {
            return Ok(view.clone());
        }
        obj_not_found_err!("Table", name)
    }

    /// Returns true iff the session has for the given identifier
    pub fn has_table(&self, name: &Identifier) -> bool {
        if name.has_namespace() {
            return false;
        }
        return self.state().tables.contains_key(&name.name);
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::empty()
    }
}
