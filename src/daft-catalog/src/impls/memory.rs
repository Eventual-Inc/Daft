use std::sync::{Arc, RwLock};

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_logical_plan::{InMemoryInfo, LogicalPlan, LogicalPlanBuilder, SourceInfo, ops::Source};
use daft_micropartition::{
    MicroPartition,
    partitioning::{MicroPartitionSet, PartitionSet},
};
use indexmap::{IndexMap, map::Entry};

use crate::{
    Catalog, FunctionRef, Identifier, Table, TableRef,
    error::{CatalogError, CatalogResult},
    pattern::{match_pattern, parse_qualified_pattern},
};

/// A namespace path. The root namespace is represented by an empty path.
type Namespace = Vec<String>;
type NamespaceTableMap = IndexMap<Namespace, IndexMap<String, Arc<MemoryTable>>>;
type FunctionMap = IndexMap<String, FunctionRef>;

/// A catalog entirely stored in-memory.
///
/// Supports tables without namespaces or nested namespaces of arbitrary depth.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MemoryCatalog {
    name: String,
    /// map of namespace path -> table name -> table
    tables: Arc<RwLock<NamespaceTableMap>>,
    /// map of function name -> function
    functions: Arc<RwLock<FunctionMap>>,
}

impl MemoryCatalog {
    pub fn new(name: String) -> Self {
        let mut tables = IndexMap::new();

        tables.insert(Vec::new(), IndexMap::new());

        Self {
            name,
            tables: Arc::new(RwLock::new(tables)),
            functions: Arc::new(RwLock::new(IndexMap::new())),
        }
    }

    /// Splits a table identifier into its namespace path and table name.
    fn split_table_ident(ident: &Identifier) -> (Namespace, &str) {
        let namespace = ident
            .qualifier()
            .map(<[String]>::to_vec)
            .unwrap_or_default();

        (namespace, ident.name())
    }

    /// Returns the full path of a namespace identifier.
    fn namespace_path(ident: &Identifier) -> Namespace {
        let mut path = ident
            .qualifier()
            .map(<[String]>::to_vec)
            .unwrap_or_default();
        path.push(ident.name().to_string());
        path
    }
}

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MemoryTable {
    name: String,
    info: Arc<RwLock<InMemoryInfo>>,
}

impl MemoryTable {
    pub fn new(name: String, schema: SchemaRef) -> DaftResult<Self> {
        let pset = Arc::new(MicroPartitionSet::empty());
        pset.set_partition(0, &Arc::new(MicroPartition::empty(Some(schema.clone()))))?;

        let cache_entry = daft_context::partition_cache::put_partition_set_into_cache(pset)?;
        let cache_key = cache_entry.key();

        let info = InMemoryInfo::new(schema, cache_key, Some(cache_entry), 0, 0, 0, None, None);

        Ok(Self {
            name,
            info: Arc::new(RwLock::new(info)),
        })
    }
}

impl Catalog for MemoryCatalog {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn create_function(&self, ident: &Identifier, function: FunctionRef) -> CatalogResult<()> {
        let name = ident.name().to_string();

        if self.functions.read().unwrap().contains_key(&name) {
            return Err(CatalogError::obj_already_exists("function", ident));
        }

        self.functions.write().unwrap().insert(name, function);

        Ok(())
    }

    fn create_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        let namespace = Self::namespace_path(ident);

        let mut tables = self.tables.write().unwrap();

        match tables.entry(namespace) {
            Entry::Vacant(slot) => {
                slot.insert(IndexMap::new());
                Ok(())
            }
            Entry::Occupied(_) => Err(CatalogError::obj_already_exists("namespace", ident)),
        }
    }

    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef> {
        let (namespace, table_name) = Self::split_table_ident(ident);

        // Build the table before taking the lock: MemoryTable::new registers
        // the partition set with the (Python) runner and can release the GIL,
        // so holding the lock across this call can deadlock with a concurrent
        // creator that holds the GIL while waiting on the lock.
        let table = Arc::new(MemoryTable::new(table_name.to_string(), schema)?);

        // Insert with conflict detection: entry() reports "already exists"
        // from the insert operation itself, so there is no separate check
        // that could race with a concurrent creator. The write lock keeps
        // the entry lookup and insert atomic across threads.
        let mut tables = self.tables.write().unwrap();

        let namespace_tables =
            tables
                .get_mut(&namespace)
                .ok_or_else(|| CatalogError::ObjectNotFound {
                    type_: "namespace".to_string(),
                    ident: Identifier::new(namespace.clone()).to_string(),
                })?;

        match namespace_tables.entry(table_name.to_string()) {
            Entry::Vacant(slot) => {
                slot.insert(table.clone());
            }
            Entry::Occupied(_) => {
                return Err(CatalogError::obj_already_exists("table", ident));
            }
        }

        Ok(table)
    }

    fn drop_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        let namespace = Self::namespace_path(ident);

        let mut tables = self.tables.write().unwrap();

        // Drop the namespace and every namespace nested beneath it.
        let mut dropped = false;
        tables.retain(|candidate, _| {
            if candidate.len() >= namespace.len() && candidate[..namespace.len()] == namespace[..] {
                dropped = true;
                false
            } else {
                true
            }
        });

        if dropped {
            Ok(())
        } else {
            Err(CatalogError::obj_not_found("namespace", ident))
        }
    }

    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()> {
        let (namespace, table_name) = Self::split_table_ident(ident);

        let mut tables = self.tables.write().unwrap();
        let Some(namespace_tables) = tables.get_mut(&namespace) else {
            return Err(CatalogError::obj_not_found("table", ident));
        };

        match namespace_tables.shift_remove(table_name) {
            Some(_) => Ok(()),
            None => Err(CatalogError::obj_not_found("table", ident)),
        }
    }

    fn get_function(&self, ident: &Identifier) -> CatalogResult<FunctionRef> {
        let name = ident.name();

        self.functions
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::obj_not_found("function", ident))
    }

    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef> {
        let (namespace, table_name) = Self::split_table_ident(ident);

        self.tables
            .read()
            .unwrap()
            .get(&namespace)
            .and_then(|namespace_tables| {
                namespace_tables
                    .get(table_name)
                    .map(|t| t.clone() as TableRef)
            })
            .ok_or_else(|| CatalogError::obj_not_found("table", ident))
    }

    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool> {
        let namespace = Self::namespace_path(ident);

        Ok(self.tables.read().unwrap().contains_key(&namespace))
    }

    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool> {
        let (namespace, table_name) = Self::split_table_ident(ident);

        Ok(self
            .tables
            .read()
            .unwrap()
            .get(&namespace)
            .is_some_and(|namespace_tables| namespace_tables.contains_key(table_name)))
    }

    fn list_namespaces(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        Ok(self
            .tables
            .read()
            .unwrap()
            .keys()
            .filter(|namespace| !namespace.is_empty())
            .filter(|namespace| pattern.is_none_or(|pat| match_pattern(&namespace.join("."), pat)))
            .map(|namespace| Identifier::new(namespace.clone()))
            .collect())
    }

    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        let tables = self.tables.read().unwrap();

        let (namespace_filter, table_pattern) = match pattern {
            Some(pat) => {
                let parsed = parse_qualified_pattern(pat);
                (parsed.namespace, Some(parsed.table))
            }
            None => (None, None),
        };

        let mut result = Vec::new();
        for (namespace, namespace_tables) in tables.iter() {
            if namespace_filter.is_some_and(|filter| namespace.join(".") != filter) {
                continue;
            }

            let parts = namespace.clone();
            for table_name in namespace_tables.keys() {
                if table_pattern.is_none_or(|pat| match_pattern(table_name, pat)) {
                    let mut parts = parts.clone();
                    parts.push(table_name.clone());
                    result.push(Identifier::new(parts));
                }
            }
        }

        Ok(result)
    }

    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        use pyo3::{intern, types::PyAnyMethods};

        use crate::python::PyCatalog;

        let pycatalog = PyCatalog(Arc::new(self.clone()));

        Ok(py
            .import(intern!(py, "daft.catalog.__internal"))?
            .getattr("MemoryCatalog")?
            .call1((pycatalog,))?
            .unbind())
    }
}

impl Table for MemoryTable {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn schema(&self) -> CatalogResult<SchemaRef> {
        Ok(self.info.read().unwrap().source_schema.clone())
    }

    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder> {
        let info = self.info.read().unwrap().clone();

        Ok(Arc::new(LogicalPlan::Source(Source::new(
            info.source_schema.clone(),
            Arc::new(SourceInfo::InMemory(info)),
        )))
        .into())
    }

    fn append(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()> {
        let append_plan = self.to_logical_plan()?.concat(&plan)?;

        self.overwrite(append_plan, options)
    }

    #[cfg(feature = "python")]
    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        _options: IndexMap<String, Literal>,
    ) -> CatalogResult<()> {
        use common_error::DaftError;

        let schema = { self.info.read().unwrap().source_schema.clone() };

        if plan.schema() != schema {
            return Err(DaftError::SchemaMismatch(format!("Expected overwritten table to preserve the schema, found:\nTable schema:\n{}\nNew schema:\n{}", schema, plan.schema())).into());
        }

        let pset = MicroPartitionSet::empty();
        let runner = daft_runners::get_or_create_runner()?;
        pyo3::Python::attach(|py| {
            for (i, res) in runner.run_iter_tables(py, plan, None)?.enumerate() {
                let mp = res?;
                pset.set_partition(i, &mp)?;
            }

            Ok::<_, DaftError>(())
        })?;

        let num_partitions = pset.num_partitions();
        let size_bytes = pset.size_bytes()?;
        let num_rows = pset.len();

        let cache_entry =
            daft_context::partition_cache::put_partition_set_into_cache(Arc::new(pset))?;

        let cache_key = cache_entry.key();

        let new_info = InMemoryInfo::new(
            schema,
            cache_key,
            Some(cache_entry),
            num_partitions,
            size_bytes,
            num_rows,
            None,
            None,
        );

        *self.info.write().unwrap() = new_info;

        Ok(())
    }

    #[cfg(not(feature = "python"))]
    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()> {
        unimplemented!("MemoryTable.overwrite requires Python")
    }

    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        use pyo3::{intern, types::PyAnyMethods};

        use crate::python::PyTable;

        let pytable = PyTable(Arc::new(self.clone()));

        Ok(py
            .import(intern!(py, "daft.catalog.__internal"))?
            .getattr("MemoryTable")?
            .call1((pytable,))?
            .unbind())
    }
}
