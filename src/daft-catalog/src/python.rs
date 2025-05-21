#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use daft_core::{prelude::SchemaRef, python::PySchema};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use pyo3::{exceptions::PyIndexError, intern, prelude::*, types::PyList};

use crate::{
    error::CatalogResult, Catalog, CatalogRef, Identifier, Table, TableRef, TableSource, View,
};

/// PyCatalog wraps a `daft.catalog.Catalog` implementation (py->rust).
#[derive(Debug)]
pub struct PyCatalog(PyObject);

impl From<PyObject> for PyCatalog {
    fn from(obj: PyObject) -> Self {
        Self(obj)
    }
}

impl PyCatalog {
    pub fn wrap(obj: PyObject) -> CatalogRef {
        Arc::new(Self::from(obj))
    }
}

impl Catalog for PyCatalog {
    fn name(&self) -> String {
        Python::with_gil(|py| {
            // catalog = 'python catalog object'
            let catalog = self.0.bind(py);
            // name = catalog.name
            let name = catalog
                .getattr(intern!(py, "name"))
                .expect("Catalog.name should never fail");
            let name: String = name.extract().expect("name must be a string");
            name
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.0.clone_ref(py))
    }

    fn create_namespace(&mut self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_create_namespace"), (ident,))?;
            Ok(())
        })
    }

    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            Ok(catalog
                .call_method1(intern!(py, "_has_namespace"), (ident,))?
                .extract()?)
        })
    }

    fn drop_namespace(&mut self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_drop_namespace"), (ident,))?;
            Ok(())
        })
    }

    fn list_namespaces(&self, prefix: Option<&Identifier>) -> CatalogResult<Vec<Identifier>> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let prefix = prefix
                .map(|p| PyIdentifier(p.clone()).to_pyobj(py))
                .transpose()?;
            let namespaces_py = catalog.call_method1(intern!(py, "_list_namespaces"), (prefix,))?;
            Ok(namespaces_py
                .downcast::<PyList>()
                .expect("Catalog._list_namespaces must return a list")
                .into_iter()
                .map(|ident| {
                    Ok(ident
                        .getattr(intern!(py, "_ident"))?
                        .extract::<PyIdentifier>()?
                        .0)
                })
                .collect::<PyResult<Vec<_>>>()?)
        })
    }

    fn create_table(&mut self, ident: &Identifier, schema: &SchemaRef) -> CatalogResult<TableRef> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            let schema = PySchema {
                schema: schema.clone(),
            };
            let schema_py = py
                .import(intern!(py, "daft.schema"))?
                .getattr(intern!(py, "Schema"))?
                .call_method1(intern!(py, "_from_pyschema"), (schema,))?;

            let table = catalog.call_method1(intern!(py, "_create_table"), (ident, schema_py))?;
            Ok(Arc::new(PyTable(table.unbind())) as Arc<dyn Table>)
        })
    }

    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            Ok(catalog
                .call_method1(intern!(py, "_has_table"), (ident,))?
                .extract()?)
        })
    }

    fn drop_table(&mut self, ident: &Identifier) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            catalog.call_method1(intern!(py, "_drop_table"), (ident,))?;
            Ok(())
        })
    }

    fn list_tables(&self, prefix: Option<&Identifier>) -> CatalogResult<Vec<Identifier>> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let prefix = prefix
                .map(|p| PyIdentifier(p.clone()).to_pyobj(py))
                .transpose()?;
            let namespaces_py = catalog.call_method1(intern!(py, "_list_tables"), (prefix,))?;
            Ok(namespaces_py
                .downcast::<PyList>()
                .expect("Catalog._list_tables must return a list")
                .into_iter()
                .map(|ident| {
                    Ok(ident
                        .getattr(intern!(py, "_ident"))?
                        .extract::<PyIdentifier>()?
                        .0)
                })
                .collect::<PyResult<Vec<_>>>()?)
        })
    }

    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef> {
        Python::with_gil(|py| {
            let catalog = self.0.bind(py);
            let ident = PyIdentifier(ident.clone()).to_pyobj(py)?;
            let table = catalog.call_method1(intern!(py, "_get_table"), (ident,))?;
            Ok(Arc::new(PyTable(table.unbind())) as Arc<dyn Table>)
        })
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
/// PyTableWrapper wraps a `daft.catalog.Table` implementation (py->rust).
#[derive(Debug)]
pub struct PyTable(PyObject);

impl From<PyObject> for PyTable {
    fn from(obj: PyObject) -> Self {
        Self(obj)
    }
}

impl PyTable {
    pub fn wrap(obj: PyObject) -> TableRef {
        Arc::new(Self::from(obj))
    }
}

impl Table for PyTable {
    fn name(&self) -> String {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            table
                .getattr(intern!(py, "name"))
                .expect("Table.name() should never fail")
                .extract()
                .expect("Table.name() must return a string")
        })
    }

    fn schema(&self) -> CatalogResult<SchemaRef> {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            let schema_py = table.call_method0(intern!(py, "schema"))?;
            let schema = schema_py
                .getattr(intern!(py, "_schema"))?
                .downcast::<PySchema>()
                .expect("downcast to PySchema failed")
                .borrow();
            Ok(schema.schema.clone())
        })
    }

    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder> {
        Python::with_gil(|py| {
            // table = 'python table object'
            let table = self.0.bind(py);
            // df = table.read()
            let df = table.call_method0(intern!(py, "read"))?;
            // builder = df._builder()
            let builder_py = df
                .getattr(intern!(py, "_builder"))?
                .getattr(intern!(py, "_builder"))?;
            // builder as PyLogicalPlanBuilder
            let builder = builder_py
                .downcast::<PyLogicalPlanBuilder>()
                .expect("downcast to PyLogicalPlanBuilder failed")
                .borrow();
            Ok(builder.builder.clone())
        })
    }

    fn write(&self, plan: LogicalPlanBuilder, mode: &str) -> CatalogResult<()> {
        Python::with_gil(|py| {
            let table = self.0.bind(py);
            let plan_py = PyLogicalPlanBuilder { builder: plan };
            let df = py
                .import(intern!(py, "daft.dataframe"))?
                .getattr(intern!(py, "DataFrame"))?
                .call1((plan_py,))?;
            table.call_method1(intern!(py, "write"), (df, mode))?;
            Ok(())
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.0.clone_ref(py))
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
    parent.add_class::<PyIdentifier>()?;
    parent.add_class::<PyTableSource>()?;
    Ok(())
}

#[pymethods]
impl View {
    #[pyo3(name = "name")]
    fn name_py(&self) -> &str {
        self.name()
    }

    #[pyo3(name = "plan")]
    fn plan_py(&self) -> PyLogicalPlanBuilder {
        PyLogicalPlanBuilder::new(self.plan())
    }
}
