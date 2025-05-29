use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_error::DaftResult;
use daft_context::get_context;
use daft_core::prelude::SchemaRef;
use daft_dsl::LiteralValue;
use daft_logical_plan::{ops::Source, InMemoryInfo, LogicalPlan, LogicalPlanBuilder, SourceInfo};
use daft_micropartition::partitioning::{MicroPartitionSet, PartitionSet};

use crate::{
    error::{CatalogError, CatalogResult},
    Catalog, Identifier, Table, TableRef,
};

#[derive(Clone, Debug)]
pub struct MemoryCatalog {
    name: String,
    tables: Arc<RwLock<HashMap<String, Arc<MemoryTable>>>>,
}

impl MemoryCatalog {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemoryTable {
    name: String,
    info: Arc<RwLock<InMemoryInfo>>,
}

impl MemoryTable {
    pub fn new(name: String, schema: SchemaRef) -> DaftResult<Self> {
        let pset = Arc::new(MicroPartitionSet::empty());

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

    fn create_namespace(&self, _ident: &Identifier) -> CatalogResult<()> {
        Err(CatalogError::unsupported(
            "MemoryCatalog does not support namespaces".to_string(),
        ))
    }

    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef> {
        if ident.has_qualifier() {
            return Err(CatalogError::unsupported(
                "MemoryCatalog does not support tables with qualifiers".to_string(),
            ));
        }

        let name = ident.name();

        if self.tables.read().unwrap().contains_key(name) {
            return Err(CatalogError::obj_already_exists("table", ident));
        }

        let table = Arc::new(MemoryTable::new(name.to_string(), schema)?);

        self.tables
            .write()
            .unwrap()
            .insert(name.to_string(), table.clone());

        Ok(table)
    }

    fn drop_namespace(&self, _ident: &Identifier) -> CatalogResult<()> {
        Err(CatalogError::unsupported(
            "MemoryCatalog does not support namespaces".to_string(),
        ))
    }

    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()> {
        if ident.has_qualifier() {
            return Err(CatalogError::obj_not_found("table", ident));
        }

        let name = ident.name();

        match self.tables.write().unwrap().remove(name) {
            Some(_) => Ok(()),
            None => Err(CatalogError::obj_not_found("table", ident)),
        }
    }

    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef> {
        if ident.has_qualifier() {
            return Err(CatalogError::obj_not_found("table", ident));
        }

        let name = ident.name();

        self.tables
            .read()
            .unwrap()
            .get(name)
            .map(|t| t.clone() as TableRef)
            .ok_or_else(|| CatalogError::obj_not_found("table", ident))
    }

    fn has_namespace(&self, _ident: &Identifier) -> CatalogResult<bool> {
        Ok(false)
    }

    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool> {
        if ident.has_qualifier() {
            return Ok(false);
        }

        let name = ident.name();

        Ok(self.tables.read().unwrap().contains_key(name))
    }

    fn list_namespaces(&self, _pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        Ok(vec![])
    }

    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>> {
        if let Some(pat) = pattern {
            Ok(self
                .tables
                .read()
                .unwrap()
                .keys()
                .filter(|name| name.starts_with(pat))
                .map(Identifier::simple)
                .collect())
        } else {
            Ok(self
                .tables
                .read()
                .unwrap()
                .keys()
                .map(Identifier::simple)
                .collect())
        }
    }

    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject> {
        use pyo3::{intern, types::PyAnyMethods};

        use crate::python::PyCatalog;

        let pycatalog = PyCatalog(Arc::new(self.clone()));

        Ok(py
            .import(intern!(py, "daft.catalog.__rust"))?
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
        options: HashMap<String, LiteralValue>,
    ) -> CatalogResult<()> {
        let append_plan = self.to_logical_plan()?.concat(&plan)?;

        self.overwrite(append_plan, options)
    }

    #[cfg(feature = "python")]
    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        _options: HashMap<String, LiteralValue>,
    ) -> CatalogResult<()> {
        use common_error::DaftError;

        let schema = { self.info.read().unwrap().source_schema.clone() };

        if plan.schema() != schema {
            return Err(DaftError::SchemaMismatch(format!("Expected overwritten table to preserve the schema, found:\nTable schema:\n{}\nNew schema:\n{}", schema, plan.schema())).into());
        }

        let runner = get_context().get_or_create_runner()?;

        let pset = MicroPartitionSet::empty();
        pyo3::Python::with_gil(|py| {
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
        options: HashMap<String, LiteralValue>,
    ) -> CatalogResult<()> {
        unimplemented!("MemoryTable.overwrite requires Python")
    }

    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject> {
        use pyo3::{intern, types::PyAnyMethods};

        use crate::python::PyTable;

        let pytable = PyTable(Arc::new(self.clone()));

        Ok(py
            .import(intern!(py, "daft.catalog.__rust"))?
            .getattr("MemoryTable")?
            .call1((pytable,))?
            .unbind())
    }
}
