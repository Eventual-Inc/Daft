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
        if self.state().catalogs.is_empty() {
            // use only catalog as the current catalog
            self.state_mut().options.curr_catalog = Some(alias.clone());
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
    /// ```text
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
    pub fn current_catalog(&self) -> Result<Option<CatalogRef>> {
        if let Some(catalog) = &self.state().options.curr_catalog {
            self.get_catalog(catalog).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Returns the session's current namespace.
    pub fn current_namespace(&self) -> Result<Option<Vec<String>>> {
        Ok(self.state().options.curr_namespace.clone())
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
        // cleanup session state
        if self.state().catalogs.is_empty() {
            self.set_catalog(None)?;
        }
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
        //
        // Rule 0: check temp tables.
        if !name.has_qualifier() {
            if let Some(view) = self.state().tables.get(&name.name) {
                return Ok(view.clone());
            }
        }
        //
        // Use session state, but error if there's no catalog and the table was not in temp tables.
        let curr_catalog = match self.current_catalog()? {
            Some(catalog) => catalog,
            None => obj_not_found_err!("Table", name),
        };
        let curr_namespace = self.current_namespace()?;
        //
        // Rule 1: try to resolve using the current catalog and current schema.
        if let Some(qualifier) = curr_namespace {
            if let Some(table) = curr_catalog.get_table(&name.qualify(qualifier))? {
                return Ok(table.into());
            };
        }
        //
        // Rule 2: try to resolve as schema-qualified using the current catalog.
        if let Some(table) = curr_catalog.get_table(name)? {
            return Ok(table.into());
        }
        //
        // The next resolution rule requires a qualifier.
        if !name.has_qualifier() {
            obj_not_found_err!("Table", name)
        }
        //
        // Rule 3: try to resolve as catalog-qualified.
        if let Ok(catalog) = self.get_catalog(&name.qualifier[0]) {
            if let Some(table) = catalog.get_table(&name.drop(1))? {
                return Ok(table.into());
            }
        }
        obj_not_found_err!("Table", name)
    }

    /// Returns true iff the session has access to a matching catalog.
    pub fn has_catalog(&self, name: &str) -> bool {
        self.state().catalogs.exists(name)
    }

    /// Returns true iff the session has access to a matching table.
    pub fn has_table(&self, name: &Identifier) -> bool {
        self.get_table(name).is_ok()
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
    pub fn set_catalog(&self, ident: Option<&str>) -> Result<()> {
        if let Some(ident) = ident {
            if !self.has_catalog(ident) {
                obj_not_found_err!("Catalog", &ident.into())
            }
            self.state_mut().options.curr_catalog = Some(ident.to_string());
        } else {
            self.state_mut().options.curr_catalog = None;
        }
        Ok(())
    }

    /// Sets the current_namespace (consider an Into at a later time).
    pub fn set_namespace(&self, ident: Option<&Identifier>) -> Result<()> {
        // TODO chore: update once Identifier is a Vec<String>
        if let Some(ident) = ident {
            let mut path = vec![];
            path.extend_from_slice(&ident.qualifier);
            path.push(ident.name.clone());
            self.state_mut().options.curr_namespace = Some(path);
        } else {
            self.state_mut().options.curr_namespace = None;
        }
        Ok(())
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::empty()
    }
}

/// Migrated from daft-catalog DaftMetaCatalog tests
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_catalog::View;
    use daft_core::prelude::*;
    use daft_logical_plan::{
        ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanBuilder,
        LogicalPlanRef, SourceInfo,
    };

    use super::*;

    fn mock_plan() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("text", DataType::Utf8),
                Field::new("id", DataType::Int32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
            })),
        ))
        .arced()
    }

    #[test]
    fn test_attach_table() {
        let sess = Session::empty();
        let plan = LogicalPlanBuilder::from(mock_plan());
        let view = View::from(plan).arced();

        // Register a table
        assert!(sess.attach_table(view, "test_table").is_ok());
    }

    #[test]
    fn test_get_table() {
        let sess = Session::empty();
        let plan = LogicalPlanBuilder::from(mock_plan());
        let view = View::from(plan).arced();

        sess.attach_table(view, "test_table")
            .expect("failed to attach table");

        assert!(sess.get_table(&Identifier::simple("test_table")).is_ok());
        assert!(sess
            .get_table(&Identifier::simple("non_existent_table"))
            .is_err());
    }
}
