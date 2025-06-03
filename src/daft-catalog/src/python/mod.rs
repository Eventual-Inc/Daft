#![allow(clippy::useless_conversion)]

mod wrappers;

use std::sync::Arc;

use daft_core::python::PySchema;
use daft_dsl::LiteralValue;
use daft_logical_plan::PyLogicalPlanBuilder;
use indexmap::IndexMap;
use pyo3::{exceptions::PyIndexError, intern, prelude::*, types::PyDict};
pub use wrappers::{PyCatalogWrapper, PyTableWrapper};

use crate::{
    impls::memory::{MemoryCatalog, MemoryTable},
    Catalog, CatalogRef, Identifier, Table, TableRef, TableSource,
};

#[derive(Clone)]
#[pyclass]
pub struct PyCatalog(pub CatalogRef);

#[pymethods]
impl PyCatalog {
    fn name(&self) -> String {
        self.0.name()
    }

    fn create_namespace(&self, ident: PyIdentifier) -> PyResult<()> {
        Ok(self.0.create_namespace(&ident.0)?)
    }

    fn create_table(
        &self,
        ident: PyIdentifier,
        schema: PySchema,
        py: Python,
    ) -> PyResult<PyObject> {
        self.0.create_table(&ident.0, schema.schema)?.to_py(py)
    }

    fn drop_namespace(&self, ident: PyIdentifier) -> PyResult<()> {
        Ok(self.0.drop_namespace(&ident.0)?)
    }
    fn drop_table(&self, ident: PyIdentifier) -> PyResult<()> {
        Ok(self.0.drop_table(&ident.0)?)
    }

    fn get_table(&self, ident: PyIdentifier, py: Python) -> PyResult<PyObject> {
        self.0.get_table(&ident.0)?.to_py(py)
    }

    fn has_namespace(&self, ident: PyIdentifier) -> PyResult<bool> {
        Ok(self.0.has_namespace(&ident.0)?)
    }

    fn has_table(&self, ident: PyIdentifier) -> PyResult<bool> {
        Ok(self.0.has_table(&ident.0)?)
    }

    #[pyo3(signature = (pattern=None))]
    fn list_namespaces(&self, pattern: Option<&str>) -> PyResult<Vec<PyIdentifier>> {
        Ok(self
            .0
            .list_namespaces(pattern)?
            .into_iter()
            .map(PyIdentifier)
            .collect())
    }

    #[pyo3(signature = (pattern=None))]
    fn list_tables(&self, pattern: Option<&str>) -> PyResult<Vec<PyIdentifier>> {
        Ok(self
            .0
            .list_tables(pattern)?
            .into_iter()
            .map(PyIdentifier)
            .collect())
    }

    #[staticmethod]
    fn new_memory_catalog(name: String, py: Python) -> PyResult<PyObject> {
        MemoryCatalog::new(name).to_py(py)
    }
}

#[derive(Clone)]
#[pyclass]
pub struct PyTable(pub TableRef);

impl PyTable {
    fn pydict_to_options(
        options: Option<&Bound<PyDict>>,
    ) -> PyResult<IndexMap<String, LiteralValue>> {
        if let Some(options) = options {
            options
                .iter()
                .map(|(key, val)| {
                    let key = key.extract()?;
                    let val = daft_dsl::python::literal_value(val)?;

                    Ok((key, val))
                })
                .collect()
        } else {
            Ok(IndexMap::new())
        }
    }
}

#[pymethods]
impl PyTable {
    fn name(&self) -> String {
        self.0.name()
    }

    fn schema(&self) -> PyResult<PySchema> {
        Ok(self.0.schema()?.into())
    }

    fn to_logical_plan(&self) -> PyResult<PyLogicalPlanBuilder> {
        Ok(self.0.to_logical_plan()?.into())
    }

    #[pyo3(signature = (plan, **options))]
    fn append(&self, plan: PyLogicalPlanBuilder, options: Option<&Bound<PyDict>>) -> PyResult<()> {
        let options = Self::pydict_to_options(options)?;

        Ok(self.0.append(plan.builder, options)?)
    }

    #[pyo3(signature = (plan, **options))]
    fn overwrite(
        &self,
        plan: PyLogicalPlanBuilder,
        options: Option<&Bound<PyDict>>,
    ) -> PyResult<()> {
        let options = Self::pydict_to_options(options)?;

        Ok(self.0.overwrite(plan.builder, options)?)
    }

    #[staticmethod]
    fn new_memory_table(name: String, schema: PySchema, py: Python) -> PyResult<PyObject> {
        MemoryTable::new(name, schema.schema)?.to_py(py)
    }
}

pub fn pyobj_to_catalog(obj: Bound<PyAny>) -> PyResult<CatalogRef> {
    if obj.is_instance_of::<PyCatalog>() {
        Ok(obj.extract::<PyCatalog>()?.0)
    } else {
        Ok(Arc::new(PyCatalogWrapper(obj.unbind())))
    }
}

pub fn pyobj_to_table(obj: Bound<PyAny>) -> PyResult<TableRef> {
    if obj.is_instance_of::<PyTable>() {
        Ok(obj.extract::<PyTable>()?.0)
    } else {
        Ok(Arc::new(PyTableWrapper(obj.unbind())))
    }
}

/// PyIdentifier maps identifier.py to identifier.rs
#[pyclass(sequence)]
#[derive(Debug, Clone)]
pub struct PyIdentifier(Identifier);

#[pymethods]
impl PyIdentifier {
    #[new]
    pub fn new(parts: Vec<String>) -> PyIdentifier {
        Identifier::new(parts).into()
    }

    #[staticmethod]
    pub fn from_sql(input: &str, normalize: bool) -> PyResult<PyIdentifier> {
        Ok(Identifier::from_sql(input, normalize)?.into())
    }

    pub fn eq(&self, other: &Self) -> PyResult<bool> {
        Ok(self.0.eq(&other.0))
    }

    pub fn getitem(&self, index: isize) -> PyResult<String> {
        let mut i = index;
        let len = self.0.len();
        if i < 0 {
            // negative index
            i = (len as isize) + index;
        }
        if i < 0 || len <= i as usize {
            // out of range
            return Err(PyIndexError::new_err(i));
        }
        Ok(self.0.get(i as usize).to_string())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.0.len())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.0))
    }
}

impl PyIdentifier {
    pub fn to_pyobj(self, py: Python) -> PyResult<Bound<PyAny>> {
        py.import(intern!(py, "daft.catalog"))?
            .getattr(intern!(py, "Identifier"))?
            .call_method1(intern!(py, "_from_pyidentifier"), (self,))
    }
}

impl From<Identifier> for PyIdentifier {
    fn from(value: Identifier) -> Self {
        Self(value)
    }
}

impl AsRef<Identifier> for PyIdentifier {
    fn as_ref(&self) -> &Identifier {
        &self.0
    }
}

/// PyTableSource wraps either a schema or dataframe.
#[pyclass]
pub struct PyTableSource(TableSource);

impl From<TableSource> for PyTableSource {
    fn from(source: TableSource) -> Self {
        Self(source)
    }
}

/// PyTableSource -> TableSource
impl AsRef<TableSource> for PyTableSource {
    fn as_ref(&self) -> &TableSource {
        &self.0
    }
}

#[pymethods]
impl PyTableSource {
    #[staticmethod]
    pub fn from_pyschema(schema: PySchema) -> PyTableSource {
        Self(TableSource::Schema(schema.schema))
    }

    #[staticmethod]
    pub fn from_pybuilder(view: &PyLogicalPlanBuilder) -> PyTableSource {
        Self(TableSource::View(view.builder.build()))
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyCatalog>()?;
    parent.add_class::<PyTable>()?;
    parent.add_class::<PyIdentifier>()?;
    parent.add_class::<PyTableSource>()?;
    Ok(())
}
