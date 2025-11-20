use std::sync::{Arc, RwLock};

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_logical_plan::{InMemoryInfo, LogicalPlan, LogicalPlanBuilder, SourceInfo, ops::Source};
use daft_micropartition::{
    MicroPartition,
    partitioning::{MicroPartitionSet, PartitionSet},
};
use indexmap::IndexMap;

use crate::{
    Catalog, Identifier, Table, TableRef,
    error::{CatalogError, CatalogResult},
};

type NamespaceTableMap = IndexMap<Option<String>, IndexMap<String, Arc<MemoryTable>>>;

/// A catalog entirely stored in-memory.
///
/// Supports tables without namespaces or with a single level namespace.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MemoryCatalog {
    name: String,
    /// map of optional namespace -> table name -> table
    tables: Arc<RwLock<NamespaceTableMap>>,
}

impl MemoryCatalog {
    pub fn new(name: String) -> Self {
        let mut tables = IndexMap::new();

        tables.insert(None, IndexMap::new());

        Self {
            name,
            tables: Arc::new(RwLock::new(tables)),
        }
    }

    /// Gets the optional namespace and table name from the ident
    fn split_table_ident(ident: &Identifier) -> CatalogResult<(Option<String>, &str)> {
        let namespace = ident
            .qualifier()
            .map(|q| {
                if let [namespace] = q {
                    Ok(namespace.clone())
                } else {
                    Err(CatalogError::unsupported(
                        "MemoryCatalog does not support nested namespaces",
                    ))
                }
            })
            .transpose()?;
        let table_name = ident.name();

        Ok((namespace, table_name))
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

    fn create_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        if ident.has_qualifier() {
            return Err(CatalogError::unsupported(
                "MemoryCatalog does not support nested namespaces",
            ));
        }

        let namespace = Some(ident.name().to_string());

        if self.tables.read().unwrap().contains_key(&namespace) {
            return Err(CatalogError::obj_already_exists("namespace", ident));
        }

        self.tables
            .write()
            .unwrap()
            .insert(namespace, IndexMap::new());

        Ok(())
    }

    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef> {
        let (namespace, table_name) = Self::split_table_ident(ident)?;

        {
            let tables = self.tables.read().unwrap();

            let Some(namespace_tables) = tables.get(&namespace) else {
                return Err(CatalogError::ObjectNotFound {
                    type_: "namespace".to_string(),
                    ident: namespace.unwrap(),
                });
            };

            if namespace_tables.contains_key(table_name) {
                return Err(CatalogError::obj_already_exists("table", ident));
            }
        }

        let table = Arc::new(MemoryTable::new(table_name.to_string(), schema)?);

        self.tables
            .write()
            .unwrap()
            .get_mut(&namespace)
            .unwrap()
            .insert(table_name.to_string(), table.clone());

        Ok(table)
    }

    fn drop_namespace(&self, ident: &Identifier) -> CatalogResult<()> {
        if ident.has_qualifier() {
            return Err(CatalogError::obj_not_found("namespace", ident));
        }

        let namespace = Some(ident.name().to_string());

        match self.tables.write().unwrap().shift_remove(&namespace) {
            Some(_) => Ok(()),
            None => Err(CatalogError::obj_not_found("namespace", ident)),
        }
    }

    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()> {
        let (namespace, table_name) = Self::split_table_ident(ident)?;

        let mut tables = self.tables.write().unwrap();
        let Some(namespace_tables) = tables.get_mut(&namespace) else {
            return Err(CatalogError::obj_not_found("table", ident));
        };

        match namespace_tables.shift_remove(table_name) {
            Some(_) => Ok(()),
            None => Err(CatalogError::obj_not_found("table", ident)),
        }
    }

    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef> {
        let (namespace, table_name) = Self::split_table_ident(ident)?;

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
        if ident.has_qualifier() {
            return Ok(false);
        }

        // works because we only support a single-level namespace
        let namespace = ident.name();

        Ok(self
            .tables
            .read()
            .unwrap()
            .contains_key(&Some(namespace.to_string())))
    }

    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool> {
        let Ok((namespace, table_name)) = Self::split_table_ident(ident) else {
            return Ok(false);
        };

        Ok(self
            .tables
            .read()
            .unwrap()
            .get(&namespace)
            .is_some_and(|namespace_tables| namespace_tables.contains_key(table_name)))
    }

    fn list_namespaces(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        if pattern.is_some() {
            return Err(CatalogError::unsupported(
                "MemoryCatalog.list_namespaces does not support specifying a pattern.",
            ));
        }

        Ok(self
            .tables
            .read()
            .unwrap()
            .keys()
            .filter_map(|namespace| namespace.as_ref().map(Identifier::simple))
            .collect())
    }

    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        let tables = self.tables.read().unwrap();
        if let Some(pat) = pattern {
            if let Some(namespace_tables) = tables.get(&Some(pat.to_string())) {
                Ok(namespace_tables
                    .keys()
                    .map(|table_name| Identifier::new(vec![pat, table_name]))
                    .collect())
            } else {
                Ok(vec![])
            }
        } else {
            Ok(tables
                .iter()
                .flat_map(|(namespace, namespace_tables)| {
                    namespace_tables
                        .keys()
                        .map(move |table_name| match namespace {
                            Some(ns) => Identifier::new(vec![ns, table_name]),
                            None => Identifier::simple(table_name),
                        })
                })
                .collect())
        }
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
