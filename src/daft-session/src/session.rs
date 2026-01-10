use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use daft_ai::provider::ProviderRef;
use daft_catalog::{Bindings, CatalogRef, Identifier, TableRef, TableSource, View};
use daft_dsl::functions::python::WrappedUDFClass;
use uuid::Uuid;

use crate::{
    ambiguous_identifier_err,
    error::CatalogResult,
    kv::KVStoreRef,
    obj_already_exists_err, obj_not_found_err,
    options::{IdentifierMode, Options},
    unsupported_err,
};

/// Session holds all state for query planning and execution (e.g. connection).
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Session {
    /// Session state for interior mutability
    state: Arc<RwLock<SessionState>>,
}

/// Session state is to be kept internal, consider a builder.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
struct SessionState {
    /// Session identifier
    _id: String,
    /// Session options i.e. curr_catalog and curr_schema.
    options: Options,
    /// Bindings for the attached catalogs.
    catalogs: Bindings<CatalogRef>,
    /// Bindings for the attached providers.
    providers: Bindings<ProviderRef>,
    /// Bindings for the attached tables.
    tables: Bindings<TableRef>,
    /// User defined functions
    functions: Bindings<WrappedUDFClass>,
    /// Bindings for the attached KV stores.
    kv_stores: Bindings<KVStoreRef>,
}

// TODO: Session should just use a Result not CatalogResult.
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

    /// Returns true if two `Session` instances share the same underlying state.
    ///
    /// This is useful when we need to determine whether two session handles are
    /// simply different views over the same logical session (i.e. they point to
    /// the same `Arc<RwLock<SessionState>>`).
    pub fn shares_state(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Creates a new empty session
    pub fn empty() -> Self {
        let state = SessionState {
            _id: Uuid::new_v4().to_string(),
            options: Options::default(),
            catalogs: Bindings::empty(),
            providers: Bindings::empty(),
            tables: Bindings::empty(),
            functions: Bindings::empty(),
            kv_stores: Bindings::empty(),
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

    /// Attaches a function to this session, this does NOT err if it already exists.
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

    /// Attaches a provider to this session, err if already exists.
    pub fn attach_provider(&self, provider: ProviderRef, alias: String) -> CatalogResult<()> {
        if self.state().providers.contains(&alias) {
            obj_already_exists_err!("Provider", &alias.into())
        }
        if self.state().providers.is_empty() {
            // if there are no current providers, then use this provider as the default.
            self.state_mut().options.curr_provider = Some(alias.clone());
        }
        self.state_mut().providers.bind(alias, provider);
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

    /// Returns the session's current provider.
    pub fn current_provider(&self) -> CatalogResult<Option<ProviderRef>> {
        if let Some(provider) = &self.state().options.curr_provider {
            self.get_provider(provider).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Returns the session's current model.
    pub fn current_model(&self) -> CatalogResult<Option<String>> {
        Ok(self.state().options.curr_model.clone())
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

    /// Detaches a function from this session. This does NOT err if it does not exist.
    pub fn detach_function(&self, name: &str) -> CatalogResult<()> {
        self.state_mut().functions.remove(name);
        Ok(())
    }

    /// Detaches a provider from this session, err if does not exist.
    pub fn detach_provider(&self, alias: &str) -> CatalogResult<()> {
        if !self.state().providers.contains(alias) {
            obj_not_found_err!("Provider", &alias.into())
        }
        self.state_mut().providers.remove(alias);
        // cleanup session state
        if self.state().providers.is_empty() {
            self.set_provider(None)?;
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

    /// Returns the function or none if it does not exist.
    pub fn get_function(&self, name: &str) -> CatalogResult<Option<WrappedUDFClass>> {
        // TODO update missing semantics to match other session objects.
        self.state().get_function(name)
    }

    /// Returns the provider or an object not found error.
    pub fn get_provider(&self, name: &str) -> CatalogResult<ProviderRef> {
        if let Some(provider) = self.state().get_attached_provider(name)? {
            Ok(provider.clone())
        } else {
            obj_not_found_err!("Provider", &name.into())
        }
    }

    /// Returns the table or an object not found error.
    pub fn get_table(&self, name: &Identifier) -> CatalogResult<TableRef> {
        //
        // Rule 0: check temp tables.
        if !name.has_qualifier()
            && let Some(view) = self.state().get_attached_table(name.name())?
        {
            return Ok(view.clone());
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

    /// Returns true iff the session has access to a matching provider.
    pub fn has_provider(&self, name: &str) -> bool {
        self.state().providers.contains(name)
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

    /// Lists all KV stores matching the pattern.
    pub fn list_kv(&self, pattern: Option<&str>) -> CatalogResult<Vec<String>> {
        Ok(self.state().kv_stores.list(pattern))
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

    /// Sets the current_provider session property.
    pub fn set_provider(&self, ident: Option<&str>) -> CatalogResult<()> {
        if let Some(ident) = ident {
            self.state_mut().options.curr_provider = Some(ident.to_string());
        } else {
            self.state_mut().options.curr_provider = None;
        }
        Ok(())
    }

    /// Sets the current_model session property.
    pub fn set_model(&self, ident: Option<&str>) -> CatalogResult<()> {
        if let Some(ident) = ident {
            self.state_mut().options.curr_model = Some(ident.to_string());
        } else {
            self.state_mut().options.curr_model = None;
        }
        Ok(())
    }

    /// Attaches a KV store to this session, err if already exists.
    pub fn attach_kv(&self, kv_store: KVStoreRef, alias: String) -> CatalogResult<()> {
        if self.state().kv_stores.contains(&alias) {
            // If already exists, overwrite it or warn?
            // For now, let's allow overwrite if it's the same object, or error.
            // But obj_already_exists_err seems strict.
            // Actually, for interactive use, we might want to allow re-attaching.
            // But let's stick to error for now, as per original code.
            obj_already_exists_err!("KV Store", &alias.into())
        }

        let mut state = self.state_mut();

        if state.kv_stores.is_empty() {
            // if there are no current kv stores, then use this kv store as the default.
            state.options.curr_kv = Some(alias.clone());
        }
        // Bind by alias
        state.kv_stores.bind(alias.clone(), kv_store.clone());
        // Also bind by store's canonical name for name-based lookup if different from alias
        if kv_store.name() != alias {
            state.kv_stores.bind(kv_store.name().to_string(), kv_store);
        }
        Ok(())
    }

    /// Detaches a KV store from this session, err if does not exist.
    pub fn detach_kv(&self, alias: &str) -> CatalogResult<()> {
        if !self.state().kv_stores.contains(alias) {
            obj_not_found_err!("KV Store", &alias.into())
        }
        self.state_mut().kv_stores.remove(alias);
        // cleanup session state
        if self.state().kv_stores.is_empty() {
            self.set_kv(None)?;
        }
        Ok(())
    }

    /// Returns the KV store or an object not found error.
    pub fn get_kv(&self, name: &str) -> CatalogResult<KVStoreRef> {
        if let Some(kv_store) = self.state().get_attached_kv(name)? {
            Ok(kv_store.clone())
        } else {
            obj_not_found_err!("KV Store", &name.into())
        }
    }

    /// Returns true iff the session has access to a matching KV store.
    pub fn has_kv(&self, name: &str) -> bool {
        self.state().kv_stores.contains(name)
    }

    /// Sets the current_kv session property.
    pub fn set_kv(&self, ident: Option<&str>) -> CatalogResult<()> {
        if let Some(ident) = ident {
            if !self.has_kv(ident) {
                obj_not_found_err!("KV Store", &ident.into())
            }
            self.state_mut().options.curr_kv = Some(ident.to_string());
        } else {
            self.state_mut().options.curr_kv = None;
        }
        Ok(())
    }

    /// Returns the session's current KV store.
    pub fn current_kv(&self) -> CatalogResult<Option<KVStoreRef>> {
        if let Some(kv_store) = &self.state().options.curr_kv {
            self.get_kv(kv_store).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Returns an identifier normalization function based upon the session options.
    pub fn normalizer(&self) -> impl Fn(&str) -> String {
        match self.state().options.identifier_mode {
            IdentifierMode::Insensitive => str::to_string,
            IdentifierMode::Sensitive => str::to_string,
            IdentifierMode::Normalize => str::to_lowercase,
        }
    }
}

impl SessionState {
    /// Get an attached catalog by name using the session's identifier mode.
    pub fn get_attached_catalog(&self, name: &str) -> CatalogResult<Option<CatalogRef>> {
        match self.catalogs.lookup(name, self.options.lookup_mode()) {
            catalogs if catalogs.is_empty() => Ok(None),
            catalogs if catalogs.len() == 1 => Ok(Some(catalogs[0].clone())),
            catalogs => ambiguous_identifier_err!("Catalog", catalogs.iter().map(|c| c.name())),
        }
    }

    /// Get an attached provider by name using the session's identifier mode.
    pub fn get_attached_provider(&self, name: &str) -> CatalogResult<Option<ProviderRef>> {
        match self.providers.lookup(name, self.options.lookup_mode()) {
            providers if providers.is_empty() => Ok(None),
            providers if providers.len() == 1 => Ok(Some(providers[0].clone())),
            providers => ambiguous_identifier_err!("Provider", providers.iter().map(|p| p.name())),
        }
    }

    /// Get an attached table by name using the session's identifier mode.
    pub fn get_attached_table(&self, name: &str) -> CatalogResult<Option<TableRef>> {
        match self.tables.lookup(name, self.options.lookup_mode()) {
            tables if tables.is_empty() => Ok(None),
            tables if tables.len() == 1 => Ok(Some(tables[0].clone())),
            tables => ambiguous_identifier_err!("Table", tables.iter().map(|t| t.name())),
        }
    }

    /// Get an attached KV store by name using the session's identifier mode.
    pub fn get_attached_kv(&self, name: &str) -> CatalogResult<Option<KVStoreRef>> {
        match self.kv_stores.lookup(name, self.options.lookup_mode()) {
            kv_stores if kv_stores.is_empty() => Ok(None),
            kv_stores if kv_stores.len() == 1 => Ok(Some(kv_stores[0].clone())),
            kv_stores => ambiguous_identifier_err!(
                "KV Store",
                kv_stores.iter().map(|k| k.name().to_string())
            ),
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
        ClusteringSpec, LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, SourceInfo, ops::Source,
        source_info::PlaceHolderInfo,
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
        assert!(
            sess.get_table(&Identifier::simple("non_existent_table"))
                .is_err()
        );
    }
}
