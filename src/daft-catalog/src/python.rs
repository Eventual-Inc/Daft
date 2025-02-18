use std::sync::Arc;

use daft_core::prelude::SchemaRef;
use daft_logical_plan::{LogicalPlanRef, PyLogicalPlanBuilder};
use pyo3::{exceptions::PyIndexError, prelude::*};

use crate::{error::Result, global_catalog, Catalog, CatalogRef, Identifier, Table, TableRef};

/// Read a table from the specified `DaftMetaCatalog`.
///
/// TODO deprecated catalog APIs #3819
///
/// This function reads a table from a `DaftMetaCatalog` and returns a PyLogicalPlanBuilder
/// object representing the plan required to read the table.
///
/// The provided `table_identifier` can be:
///
/// 1. Name of a registered dataframe/SQL view (manually registered using `DaftMetaCatalog.register_view`)
/// 2. Name of a table within the default catalog (without inputting the catalog name) for example: `"my.table.name"`
/// 3. Name of a fully-qualified table path with the catalog name for example: `"my_catalog.my.table.name"`
///
/// Args:
///     table_identifier (str): The identifier of the table to read.
///
/// Returns:
///     PyLogicalPlanBuilder: A PyLogicalPlanBuilder object representing the table's data.
///
/// Raises:
///     DaftError: If the table cannot be read or the specified table identifier is not found.
///
/// Example:
///     >>> import daft
///     >>> df = daft.read_table("foo")
#[pyfunction]
#[pyo3(name = "read_table")]
fn py_read_table(table_identifier: &str) -> PyResult<PyLogicalPlanBuilder> {
    let logical_plan_builder = global_catalog::GLOBAL_DAFT_META_CATALOG
        .read()
        .unwrap()
        .read_table(table_identifier)?;
    Ok(PyLogicalPlanBuilder::new(logical_plan_builder))
}

/// Register a table with the global catalog.
///
/// TODO deprecated catalog APIs #3819
///
/// This function registers a table with the global `DaftMetaCatalog` using the provided
/// table identifier and logical plan.
///
/// Args:
///     table_identifier (str): The identifier to use for the registered table.
///     logical_plan (PyLogicalPlanBuilder): The logical plan representing the table's data.
///
/// Returns:
///     str: The table identifier used for registration.
///
/// Example:
///     >>> import daft
///     >>> df = daft.read_csv("data.csv")
///     >>> daft.register_table("my_table", df)
#[pyfunction]
#[pyo3(name = "register_table")]
fn py_register_table(
    table_identifier: &str,
    logical_plan: &PyLogicalPlanBuilder,
) -> PyResult<String> {
    global_catalog::GLOBAL_DAFT_META_CATALOG
        .write()
        .unwrap()
        .register_table(table_identifier, logical_plan.builder.clone())?;
    Ok(table_identifier.to_string())
}

/// Unregisters a catalog from the Daft catalog system
///
/// TODO deprecated catalog APIs #3819
///
/// This function removes a previously registered catalog from the Daft catalog system.
///
/// Args:
///     catalog_name (Optional[str]): The name of the catalog to unregister. If None, the default catalog will be unregistered.
///
/// Returns:
///     bool: True if a catalog was successfully unregistered, False otherwise.
///
/// Example:
///     >>> import daft
///     >>> daft.unregister_catalog("my_catalog")
///     True
#[pyfunction]
#[pyo3(
    name = "unregister_catalog",
    signature = (catalog_name=None)
)]
pub fn py_unregister_catalog(catalog_name: Option<&str>) -> bool {
    crate::global_catalog::unregister_catalog(catalog_name)
}

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
        todo!()
    }

    fn get_table(&self, _name: &Identifier) -> Result<Option<Box<dyn Table>>> {
        todo!()
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
    pub fn new(namespace: Vec<String>, name: String) -> PyIdentifier {
        Identifier::new(namespace, name).into()
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
        Ok(self.0.namespace[i as usize].to_string())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.0.namespace.len() + 1)
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
pub struct PyTable(TableRef);

impl PyTable {
    pub fn new(table: TableRef) -> Self {
        Self(table)
    }
}

#[pymethods]
impl PyTable {}

/// PyTableWrapper wraps a `daft.catalog.Table` implementation (py->rust).
#[derive(Debug)]
pub struct PyTableWrapper(PyObject);

impl PyTableWrapper {
    pub fn wrap(obj: PyObject) -> TableRef {
        Arc::new(Self::from(obj))
    }
}

impl Table for PyTableWrapper {
    fn get_schema(&self) -> SchemaRef {
        todo!()
    }

    fn get_logical_plan(&self) -> Result<LogicalPlanRef> {
        todo!()
    }

    fn to_py(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.0.extract(py)
    }
}

pub fn register_modules<'py>(parent: &Bound<'py, PyModule>) -> PyResult<Bound<'py, PyModule>> {
    parent.add_class::<PyCatalog>()?;
    parent.add_class::<PyIdentifier>()?;
    parent.add_class::<PyTable>()?;
    // TODO deprecated catalog APIs #3819
    let module = PyModule::new(parent.py(), "catalog")?;
    module.add_wrapped(wrap_pyfunction!(py_read_table))?;
    module.add_wrapped(wrap_pyfunction!(py_register_table))?;
    module.add_wrapped(wrap_pyfunction!(py_unregister_catalog))?;
    parent.add_submodule(&module)?;
    Ok(module)
}
