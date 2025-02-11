
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daft_catalog::{catalog::Catalogs, DataCatalog, Identifier, Name};
use daft_logical_plan::LogicalPlanBuilder;
// use options::Options;

use crate::{error::Result, obj_already_exists_err, obj_not_found_err, options::Options, unsupported_err};

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
    /// Session options i.e. curr_catalog and curr_schema.
    _options: Options,
    /// References to the available catalogs.
    catalogs: Catalogs,
    // TODO execution context
}

impl Session {
    /// Creates a new empty session
    pub fn new(id: &str) -> Self {
        let state = SessionState {
            _id: id.to_string(),
            _options: Options::default(),
            catalogs: Catalogs::empty(),
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
    pub fn attach<N : Into<Name>>(&self, name: N, catalog: Arc<dyn DataCatalog>) -> Result<()> {
        let name = name.into();
        if name.has_namespace() {
           unsupported_err!("attach catalog with namespace") 
        }
        if self.state().catalogs.exists(&name.name) {
            obj_already_exists_err!("Catalog", name)
        }
        self.state_mut().catalogs.attach(name.name, catalog);
        Ok(())
    }

    /// Dettaches a catalog from this session, err if does not exist.
    pub fn detach<I : AsRef<Identifier>>(&self, ident: &I) -> Result<()> {
        let ident = ident.as_ref();
        if ident.has_qualifier() {
            unsupported_err!("detach catalog with qualifier")
        }
        if self.state().catalogs.exists(&ident.name) {
            obj_not_found_err!("Catalog", ident)
        }
        self.state_mut().catalogs.detach(&ident.name);
        Ok(())
    }

    /// Creates a table backed by the view
    /// TODO support other table sources.
    pub fn create_table<N : Into<Name>>(&self, name: N, view: impl Into<LogicalPlanBuilder>) -> Result<()> {
        todo!()
        // self.state_mut().metastore.register_table(name, view)
    }

    /// Gets a table by identifier
    pub fn get_table(&self, ident: &Identifier) -> Result<LogicalPlanBuilder> {
        // TODO current read_table accepts an unparsed string
        let table_identifier = ident.to_string();
        // if ident.has_namespace() {
        //     unsupported!("qualified table names")
        // }
        todo!()
        // self.state().metastore.read_table(&table_identifier)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new("default")
    }
}
