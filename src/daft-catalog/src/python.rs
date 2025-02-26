use std::sync::Arc;

use daft_core::{prelude::SchemaRef, python::PySchema};
use daft_logical_plan::{LogicalPlanRef, PyLogicalPlanBuilder};
use pyo3::{exceptions::PyIndexError, intern, prelude::*};

use crate::{error::Result, Catalog, CatalogRef, Identifier, Table, TableRef, TableSource, View};

/// PyCatalog implements the Catalog ABC for some Catalog trait impl (rust->py).
#[pyclass]
pub struct PyCatalog(CatalogRef);

impl From<CatalogRef> for PyCatalog {
    fn from(catalog: CatalogRef) -> Self {
        Self(catalog)
    }
}

#[pymethods]
impl PyCatalog {
    fn name(&self) -> String {
        self.0.name()
    }
}

/// PyCatalogWrapper wraps a `daft.catalog.Catalog` implementation (py->rust).
#[derive(Debug)]
pub struct PyCatalogWrapper(PyObject);

impl From<PyObject> for PyCatalogWrapper {
    fn from(obj: PyObject) -> Self {
        Self(obj)
    }
}

impl PyCatalogWrapper {
    pub fn wrap(obj: PyObject) -> CatalogRef {
        Arc::new(Self::from(obj))
    }
}

impl Catalog for PyCatalogWrapper {
    fn name(&self) -> String {
        todo!("Catalogs do not currently hold their names")
    }

    fn get_table(&self, ident: &Identifier) -> Result<Option<Box<dyn Table>>> {
        Python::with_gil(|py| {
            // catalog = 'python catalog object'
            let catalog = self.0.bind(py);
            // TODO chore: create a python identifier here.
            let identifier = ident.to_string();
            // table = catalog.get_table(ident)
            if let Ok(table) = catalog.getattr("get_table")?.call1((identifier,)) {
                // wrap py table object so it's an impl Table
                let table = PyTableWrapper::from(table.unbind());
                Ok(Some(Box::new(table) as Box<dyn Table>))
            } else {
                // ignore table not found to make this optional
                Ok(None)
            }
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.0.extract(py)
    }
}

/// PyIdentifier maps identifier.py to identifier.rs
#[pyclass(sequence)]
#[derive(Debug, Clone)]
pub struct PyIdentifier(Identifier);

#[pymethods]
impl PyIdentifier {
    #[new]
    pub fn new(qualifier: Vec<String>, name: String) -> PyIdentifier {
        Identifier::new(qualifier, name).into()
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
        let len = self.__len__()?;
        if i < 0 {
            // negative index
            i = (len as isize) + index;
        }
        if i < 0 || len <= i as usize {
            // out of range
            return Err(PyIndexError::new_err(i));
        }
        if i as usize == len - 1 {
            // last is name
            return Ok(self.0.name.to_string());
        }
        Ok(self.0.qualifier[i as usize].to_string())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.0.qualifier.len() + 1)
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.0))
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

/// PyTable implements the `daft.catalog.Table`` ABC for some Table trait impl (rust->py).
#[pyclass]
#[allow(unused)]
pub struct PyTable(TableRef);

impl PyTable {
    pub fn new(table: TableRef) -> Self {
        Self(table)
    }
}

#[pymethods]
impl PyTable {
    /// Create an immutable table backed by the logical plan.
    #[staticmethod]
    fn from_builder(builder: &PyLogicalPlanBuilder) -> PyResult<PyTable> {
        let view = builder.builder.build();
        let view = View::from(view).arced();
        Ok(PyTable::new(view))
    }

    /// Creates a python DataFrame for this table, likely easier with python-side helpers.
    fn read(&self, py: Python<'_>) -> PyResult<PyObject> {
        // builder = 'compiled plan'
        let builder = self.0.get_logical_plan()?;
        let builder = PyLogicalPlanBuilder::new(builder.into());
        // builder = LogicalPlanBuilder.__init__(builder)
        let builder = py
            .import(intern!(py, "daft.logical.builder"))?
            .getattr(intern!(py, "LogicalPlanBuilder"))?
            .call1((builder,))?;
        // df = DataFrame.__init__(builder)
        let df = py
            .import(intern!(py, "daft.dataframe"))?
            .getattr(intern!(py, "DataFrame"))?
            .call1((builder,))?;
        // df as object
        df.extract()
    }
}

/// PyTableWrapper wraps a `daft.catalog.Table` implementation (py->rust).
#[derive(Debug)]
pub struct PyTableWrapper(PyObject);

impl From<PyObject> for PyTableWrapper {
    fn from(obj: PyObject) -> Self {
        Self(obj)
    }
}

impl PyTableWrapper {
    pub fn wrap(obj: PyObject) -> TableRef {
        Arc::new(Self::from(obj))
    }
}

impl Table for PyTableWrapper {
    fn get_schema(&self) -> SchemaRef {
        todo!("get_schema")
    }

    fn get_logical_plan(&self) -> Result<LogicalPlanRef> {
        Python::with_gil(|py| {
            // table = 'python table object'
            let table = self.0.bind(py);
            // df = table.read()
            let df = table.call_method0("read")?;
            // builder = df._builder._builder
            let builder = df.getattr("_builder")?.getattr("_builder")?;
            // builder as PyLogicalPlanBuilder
            let builder = builder
                .downcast::<PyLogicalPlanBuilder>()
                .expect("downcast to PyLogicalPlanBuilder failed")
                .borrow();
            Ok(builder.builder.plan.clone())
        })
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.0.extract(py)
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
    pub fn from_schema(schema: PySchema) -> PyTableSource {
        Self(TableSource::Schema(schema.schema))
    }

    #[staticmethod]
    pub fn from_builder(view: &PyLogicalPlanBuilder) -> PyTableSource {
        Self(TableSource::View(view.builder.build()))
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyCatalog>()?;
    parent.add_class::<PyIdentifier>()?;
    parent.add_class::<PyTable>()?;
    parent.add_class::<PyTableSource>()?;
    Ok(())
}
