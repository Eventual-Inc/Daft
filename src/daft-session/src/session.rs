use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daft_catalog::{Bindings, CatalogRef, Identifier, TableRef, TableSource, View};
use uuid::Uuid;

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
    /// Bindings for the attached tables.
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
    pub fn attach_catalog(&self, catalog: CatalogRef, alias: String) -> Result<()> {
        if self.state().catalogs.exists(&alias) {
            obj_already_exists_err!("Catalog", &alias.into())
        }
        self.state_mut().catalogs.insert(alias, catalog);
        Ok(())
    }

    /// Attaches a table to this session, err if already exists.
    pub fn attach_table(&self, table: TableRef, alias: impl Into<String>) -> Result<()> {
        let alias = alias.into();
        if self.state().tables.exists(&alias) {
            obj_already_exists_err!("Table", &alias.into())
        }
        self.state_mut().tables.insert(alias, table);
        Ok(())
    }

    /// Creates a temp table scoped to this session from an existing view.
    ///
    /// TODO feat: consider making a CreateTableSource object for more complicated options.
    ///
    /// ```
    /// CREATE [OR REPLACE] TEMP TABLE [IF NOT EXISTS] <name> <source>;
    /// ```
    pub fn create_temp_table(
        &self,
        name: impl Into<String>,
        source: &TableSource,
        replace: bool,
    ) -> Result<TableRef> {
        let name = name.into();
        if !replace && self.state().tables.exists(&name) {
            obj_already_exists_err!("Temporary table", &name.into())
        }
        // we don't have mutable temporary tables, only immutable views over dataframes.
        let table = match source {
            TableSource::Schema(_) => unsupported_err!("temporary table with schema"),
            TableSource::View(plan) => View::from(plan.clone()).arced(),
        };
        self.state_mut().tables.insert(name, table.clone());
        Ok(table)
    }

    /// Returns the session's current catalog.
    pub fn current_catalog(&self) -> Result<CatalogRef> {
        self.get_catalog(&self.state().options.curr_catalog)
    }

    /// Detaches a table from this session, err if does not exist.
    pub fn detach_table(&self, alias: &str) -> Result<()> {
        if !self.state().tables.exists(alias) {
            obj_not_found_err!("Table", &alias.into())
        }
        self.state_mut().tables.remove(alias);
        Ok(())
    }

    /// Detaches a catalog from this session, err if does not exist.
    pub fn detach_catalog(&self, alias: &str) -> Result<()> {
        if !self.state().catalogs.exists(alias) {
            obj_not_found_err!("Catalog", &alias.into())
        }
        self.state_mut().catalogs.remove(alias);
        Ok(())
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
