use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daft_catalog::{Bindings, CatalogRef, Identifier, TableRef, TableSource, View};
use daft_dsl::functions::python::WrappedUDFClass;
use uuid::Uuid;

use crate::{
    error::CatalogResult,
    obj_already_exists_err, obj_not_found_err,
    options::{IdentifierMode, Options},
    unsupported_err,
};

/// Session holds all state for query planning and execution (e.g. connection).
#[derive(Debug)]
pub struct Session {
    /// Session state for interior mutability
    state: Arc<RwLock<SessionState>>,
}

/// Session state is to be kept internal, consider a builder.
#[derive(Debug, Clone)]
struct SessionState {
    /// Session identifier
    _id: String,
    /// Session options i.e. curr_catalog and curr_schema.
    options: Options,
    /// Bindings for the attached catalogs.
    catalogs: Bindings<CatalogRef>,
    /// Bindings for the attached tables.
    tables: Bindings<TableRef>,
    /// User defined functions
    functions: Bindings<WrappedUDFClass>,
}

impl Session {
    /// Creates a new session that shares the same underlying state with the original.
    ///
    /// This method creates a new `Session` instance that contains a clone of the `Arc`
    /// pointer to the same `RwLock<SessionState>`, but does not clone the state itself.
    /// This means that any changes to the state through either session will be visible
    /// to the other session.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let original_session = Session::empty();
    /// // ... configure original_session ...
    ///
    /// // Create another session that shares state with the original
    /// let shared_session = original_session.clone_ref();
    ///
    /// // Changes through shared_session will be visible to original_session and vice versa
    /// ```
    pub fn clone_ref(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }

    /// Creates a new empty session
    pub fn empty() -> Self {
        let state = SessionState {
            _id: Uuid::new_v4().to_string(),
            options: Options::default(),
            catalogs: Bindings::empty(),
            tables: Bindings::empty(),
            functions: Bindings::empty(),
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
    pub fn attach_catalog(&self, catalog: CatalogRef, alias: String) -> CatalogResult<()> {
        if self.state().catalogs.contains(&alias) {
            obj_already_exists_err!("Catalog", &alias.into())
        }
        if self.state().catalogs.is_empty() {
            // use only catalog as the current catalog
            self.state_mut().options.curr_catalog = Some(alias.clone());
        }
        self.state_mut().catalogs.bind(alias, catalog);
        Ok(())
    }

    /// Attaches a table to this session, err if already exists.
    pub fn attach_table(&self, table: TableRef, alias: impl Into<String>) -> CatalogResult<()> {
        let alias: String = alias.into();
        if self.state().tables.contains(&alias) {
            obj_already_exists_err!("Table", &alias.into())
        }
        self.state_mut().tables.bind(alias, table);
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
    ) -> CatalogResult<TableRef> {
        let name: String = name.into();
        if !replace && self.state().tables.contains(&name) {
            obj_already_exists_err!("Temporary table", &name.into())
        }
        // we don't have mutable temporary tables, only immutable views over dataframes.
        let table = match source {
            TableSource::Schema(_) => unsupported_err!("temporary table with schema"),
            TableSource::View(plan) => Arc::new(View::new(&name, plan.clone())),
        };
        self.state_mut().tables.bind(name, table.clone());
        Ok(table)
    }

    /// Returns the session's current catalog.
    pub fn current_catalog(&self) -> CatalogResult<Option<CatalogRef>> {
        if let Some(catalog) = &self.state().options.curr_catalog {
            self.get_catalog(catalog).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Returns the session's current namespace.
    pub fn current_namespace(&self) -> CatalogResult<Option<Vec<String>>> {
        Ok(self.state().options.curr_namespace.clone())
    }

    /// Detaches a table from this session, err if does not exist.
    pub fn detach_table(&self, alias: &str) -> CatalogResult<()> {
        if !self.state().tables.contains(alias) {
            obj_not_found_err!("Table", &alias.into())
        }
        self.state_mut().tables.remove(alias);
        Ok(())
    }

    /// Detaches a catalog from this session, err if does not exist.
    pub fn detach_catalog(&self, alias: &str) -> CatalogResult<()> {
        if !self.state().catalogs.contains(alias) {
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
    pub fn get_catalog(&self, name: &str) -> CatalogResult<CatalogRef> {
        if let Some(catalog) = self.state().get_attached_catalog(name)? {
            Ok(catalog.clone())
        } else {
            obj_not_found_err!("Catalog", &name.into())
        }
    }

    /// Returns the table or an object not found error.
    pub fn get_table(&self, name: &Identifier) -> CatalogResult<TableRef> {
        //
        // Rule 0: check temp tables.
        if !name.has_qualifier() {
            if let Some(view) = self.state().get_attached_table(name.name())? {
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
            let ident = name.qualify(qualifier);
            if curr_catalog.has_table(&ident)? {
                return curr_catalog.get_table(&ident);
            }
        }
        //
        // Rule 2: try to resolve as schema-qualified using the current catalog.
        if curr_catalog.has_table(name)? {
            return curr_catalog.get_table(name);
        }
        //
        // The next resolution rule requires a qualifier.
        if !name.has_qualifier() {
            obj_not_found_err!("Table", name)
        }
        //
        // Rule 3: try to resolve as catalog-qualified.
        if let Ok(catalog) = self.get_catalog(name.get(0)) {
            let ident = name.drop(1);
            if catalog.has_table(&ident)? {
                return catalog.get_table(&ident);
            }
        }
        obj_not_found_err!("Table", name)
    }

    /// Returns true iff the session has access to a matching catalog.
    pub fn has_catalog(&self, name: &str) -> bool {
        self.state().catalogs.contains(name)
    }

    /// Returns true iff the session has access to a matching table.
    pub fn has_table(&self, name: &Identifier) -> bool {
        self.get_table(name).is_ok()
    }

    /// Lists all catalogs matching the pattern.
    pub fn list_catalogs(&self, pattern: Option<&str>) -> CatalogResult<Vec<String>> {
        Ok(self.state().catalogs.list(pattern))
    }

    /// Lists all tables matching the pattern.
    pub fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<String>> {
        Ok(self.state().tables.list(pattern))
    }

    /// Sets the current_catalog.
    pub fn set_catalog(&self, ident: Option<&str>) -> CatalogResult<()> {
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

    /// Sets the current_namespace.
    pub fn set_namespace(&self, ident: Option<&Identifier>) -> CatalogResult<()> {
        if let Some(ident) = ident {
            self.state_mut().options.curr_namespace = Some(ident.clone().path());
        } else {
            self.state_mut().options.curr_namespace = None;
        }
        Ok(())
    }

    /// Returns an identifier normalization function based upon the session options.
    pub fn normalizer(&self) -> impl Fn(&str) -> String {
        match self.state().options.identifier_mode {
            IdentifierMode::Insensitive => str::to_string,
            IdentifierMode::Sensitive => str::to_string,
            IdentifierMode::Normalize => str::to_lowercase,
        }
    }

    pub fn attach_function(
        &self,
        function: WrappedUDFClass,
        alias: Option<String>,
    ) -> CatalogResult<()> {
        #[cfg(feature = "python")]
        {
            let name = match alias {
                Some(name) => name,
                None => function.name()?,
            };

            self.state_mut().functions.bind(name, function);
            Ok(())
        }
        #[cfg(not(feature = "python"))]
        {
            Err(daft_catalog::error::CatalogError::unsupported(
                "attach_function without python",
            ))
        }
    }

    pub fn detach_function(&self, name: &str) -> CatalogResult<()> {
        self.state_mut().functions.remove(name);
        Ok(())
    }

    pub fn get_function(&self, name: &str) -> CatalogResult<Option<WrappedUDFClass>> {
        self.state().get_function(name)
    }
}

impl SessionState {
    /// Get an attached catalog by name using the session's identifier mode.
    pub fn get_attached_catalog(&self, name: &str) -> CatalogResult<Option<CatalogRef>> {
        match self.catalogs.lookup(name, self.options.find_mode()) {
            catalogs if catalogs.is_empty() => Ok(None),
            catalogs if catalogs.len() == 1 => Ok(Some(catalogs[0].clone())),
            _ => panic!("ambiguous catalog identifier"),
        }
    }

    /// Get an attached table by name using the session's identifier mode.
    pub fn get_attached_table(&self, name: &str) -> CatalogResult<Option<TableRef>> {
        match self.tables.lookup(name, self.options.find_mode()) {
            tables if tables.is_empty() => Ok(None),
            tables if tables.len() == 1 => Ok(Some(tables[0].clone())),
            _ => panic!("ambiguous table identifier"),
        }
    }

    #[cfg(feature = "python")]
    pub fn get_function(&self, name: &str) -> CatalogResult<Option<WrappedUDFClass>> {
        let mut items = self
            .functions
            .lookup(name, daft_catalog::LookupMode::Insensitive)
            .into_iter();

        if items.len() > 1 {
            let names = items
                .map(|i| i.name())
                .collect::<pyo3::PyResult<Vec<_>>>()?;

            crate::ambiguous_identifier_err!("Function", names);
        }
        Ok(items.next().cloned())
    }

    #[cfg(not(feature = "python"))]
    pub fn get_function(&self, name: &str) -> CatalogResult<Option<WrappedUDFClass>> {
        Ok(None)
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8),
            Field::new("id", DataType::Int32),
        ]));
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
        let view = Arc::new(View::new("view", plan));

        // Register a table
        assert!(sess.attach_table(view, "test_table").is_ok());
    }

    #[test]
    fn test_get_table() {
        let sess = Session::empty();
        let plan = LogicalPlanBuilder::from(mock_plan());
        let view = Arc::new(View::new("view", plan));

        sess.attach_table(view, "test_table")
            .expect("failed to attach table");

        assert!(sess.get_table(&Identifier::simple("test_table")).is_ok());
        assert!(sess
            .get_table(&Identifier::simple("non_existent_table"))
            .is_err());
    }
}
