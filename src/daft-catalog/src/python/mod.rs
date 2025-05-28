#![allow(clippy::useless_conversion)]

mod wrappers;

use daft_core::python::PySchema;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::{exceptions::PyIndexError, intern, prelude::*};
pub use wrappers::{PyCatalogWrapper, PyTableWrapper};

use crate::{CatalogRef, Identifier, TableRef, TableSource};

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
        self.0.create_table(&ident.0, &schema.schema)?.to_py(py)
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
}

#[pyclass]
pub struct PyTable(pub TableRef);

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

    fn write(&self, plan: PyLogicalPlanBuilder, mode: &str) -> PyResult<()> {
        Ok(self.0.write(plan.builder, mode)?)
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
