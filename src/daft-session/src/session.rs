use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use daft_catalog::{Bindings, Catalog, CatalogProvider, CatalogRef, Identifier, Table, TableProvider, TableRef, TableSource};
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
    /// Bindings for the attached catalogs.
    catalogs: Bindings<CatalogRef>,
    /// Bindings for the temporary tables.
    tables: Bindings<TableRef>,
    // TODO execution context
    // TODO identifier matcher for case-insensitive matching
}

impl Session {
    /// Creates a new empty session
    pub fn empty() -> Self {
        let state = SessionState {
            _id: Uuid::new_v4().to_string(),
            options: Options::default(),
            catalogs: Bindings::empty(),
            tables: Bindings::empty(),
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
    pub fn attach(&self, catalog: CatalogRef, alias: String) -> Result<()> {
        if self.state().catalogs.exists(&alias) {
            obj_already_exists_err!("Catalog", &alias.into())
        }
        self.state_mut().catalogs.insert(alias, catalog);
        Ok(())
    }

    /// Detaches a catalog from this session, err if does not exist.
    pub fn detach(&self, catalog: &str) -> Result<()> {
        if !self.state().catalogs.exists(catalog) {
            obj_not_found_err!("Catalog", &catalog.into())
        }
        self.state_mut().catalogs.remove(catalog);
        Ok(())
    }

    /// Creates a table in the current namespace with the given source.
    pub fn create_table(&self, name: Identifier, source: TableSource) -> Result<()> {
        unsupported_err!("Creating a table is not implemented, try create_temp_table.")
    }

    /// Creates a temp table scoped to this session from an existing view.
    pub fn create_temp_table(&self, name: &str, source: TableSource) -> Result<TableRef> {
        if self.state().tables.exists(name) {
            obj_already_exists_err!("Temporary table", &name.into())
        }
        // TODO update the source!! pulling double duty since we just return the original table..
        self.state_mut().tables.insert(name.to_string(), source.clone());
        Ok(source)
    }

    /// Returns the session's current catalog.
    pub fn current_catalog(&self) -> Result<CatalogRef> {
        self.get_catalog(&self.state().options.curr_catalog)
    }

    /// Returns the session's current schema.
    pub fn current_namespace(&self) -> Result<()> {
        todo!()
    }

    /// Returns the catalog or an object not found error.
    pub fn get_catalog(&self, name: &str) -> Result<CatalogRef> {
        if let Some(catalog) = self.state().catalogs.get(name) {
            Ok(catalog.clone())
        } else {
            obj_not_found_err!("Catalog", &name.into())
        }
    }

    /// Returns the table or an object not found error.
    pub fn get_table(&self, name: &Identifier) -> Result<TableRef> {
        if name.has_namespace() {
            unsupported_err!("Qualified identifiers are not yet supported")
        }
        if let Some(view) = self.state().tables.get(&name.name) {
            return Ok(view.clone());
        }
        obj_not_found_err!("Table", name)
    }

    /// Returns true iff the session has access to a matching catalog.
    pub fn has_catalog(&self, name: &str) -> bool {
        self.state().catalogs.exists(name)
    }

    /// Returns true iff the session has access to a matching table.
    pub fn has_table(&self, name: &Identifier) -> bool {
        if name.has_namespace() {
            return false;
        }
        return self.state().tables.exists(&name.name);
    }

    /// Lists all catalogs matching the pattern.
    pub fn list_catalogs(&self, pattern: Option<&str>) -> Result<Vec<String>> {
        Ok(self.state().catalogs.list(pattern))
    }

    /// Lists all tables matching the pattern.
    pub fn list_tables(&self, pattern: Option<&str>) -> Result<Vec<String>> {
        Ok(self.state().tables.list(pattern))
    }

    /// Sets the current_catalog
    pub fn set_catalog(&self, name: &str) -> Result<()> {
        if !self.has_catalog(name) {
            obj_not_found_err!("Catalog", &name.into())
        }
        self.state_mut().options.curr_catalog = name.to_string();
        Ok(())
    }
}


impl Default for Session {
    fn default() -> Self {
        Self::empty()
    }
}
