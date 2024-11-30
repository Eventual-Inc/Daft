use std::sync::Arc;

use daft_catalog::{
    errors::{Error as DaftCatalogError, Result},
    DataCatalog, DataCatalogTable,
};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use pyo3::prelude::*;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error while listing tables: {}", source))]
    ListTables { source: pyo3::PyErr },

    #[snafu(display("Error while getting table {}: {}", table_name, source))]
    GetTable {
        source: pyo3::PyErr,
        table_name: String,
    },

    #[snafu(display("Error while reading table {} into Daft: {}", table_name, source))]
    ReadTable {
        source: pyo3::PyErr,
        table_name: String,
    },
}

impl From<Error> for DaftCatalogError {
    fn from(value: Error) -> Self {
        match value {
            Error::ListTables { source } => DaftCatalogError::PythonError {
                source,
                context: "listing tables".to_string(),
            },
            Error::GetTable { source, table_name } => DaftCatalogError::PythonError {
                source,
                context: format!("getting table `{}`", table_name),
            },
            Error::ReadTable { source, table_name } => DaftCatalogError::PythonError {
                source,
                context: format!("reading table `{}`", table_name),
            },
        }
    }
}

/// Wrapper around a `daft.catalog.python_catalog.PythonCatalogTable`
pub struct PythonTable {
    table_name: String,
    table_pyobj: PyObject,
}

impl PythonTable {
    pub fn new(table_name: String, table_pyobj: PyObject) -> Self {
        Self {
            table_name,
            table_pyobj,
        }
    }
}

impl DataCatalogTable for PythonTable {
    fn to_logical_plan_builder(&self) -> daft_catalog::errors::Result<LogicalPlanBuilder> {
        Python::with_gil(|py| {
            let dataframe = self
                .table_pyobj
                .bind(py)
                .getattr("to_dataframe")
                .and_then(|to_dataframe_method| to_dataframe_method.call0())
                .with_context(|_| ReadTableSnafu {
                    table_name: self.table_name.clone(),
                })?;

            let py_logical_plan_builder = dataframe
                .getattr("_builder")
                .unwrap()
                .getattr("_builder")
                .unwrap();
            let py_logical_plan_builder = py_logical_plan_builder
                .downcast::<PyLogicalPlanBuilder>()
                .unwrap();
            Ok(py_logical_plan_builder.borrow().builder.clone())
        })
    }
}

/// Wrapper around a `daft.catalog.python_catalog.PythonCatalog`
pub struct PythonCatalog {
    python_catalog_pyobj: PyObject,
}

impl PythonCatalog {
    pub fn new(python_catalog_pyobj: PyObject) -> Self {
        Self {
            python_catalog_pyobj,
        }
    }
}

impl DataCatalog for PythonCatalog {
    fn list_tables(&self, prefix: &str) -> Result<Vec<String>> {
        Python::with_gil(|py| {
            let python_catalog = self.python_catalog_pyobj.bind(py);

            Ok(python_catalog
                .getattr("list_tables")
                .and_then(|list_tables_method| list_tables_method.call1((prefix,)))
                .and_then(|tables| tables.extract::<Vec<String>>())
                .with_context(|_| ListTablesSnafu)?)
        })
    }

    fn get_table(&self, name: &str) -> Result<Option<Box<dyn DataCatalogTable>>> {
        Python::with_gil(|py| {
            let python_catalog = self.python_catalog_pyobj.bind(py);
            let list_tables_method =
                python_catalog
                    .getattr("load_table")
                    .with_context(|_| GetTableSnafu {
                        table_name: name.to_string(),
                    })?;
            let table = list_tables_method
                .call1((name,))
                .with_context(|_| GetTableSnafu {
                    table_name: name.to_string(),
                })?;
            let python_table = PythonTable::new(name.to_string(), table.unbind());
            Ok(Some(Box::new(python_table) as Box<dyn DataCatalogTable>))
        })
    }
}

/// Registers an PythonCatalog instance
///
/// This function registers a Python-based catalog with the Daft catalog system.
///
/// Args:
///     python_catalog_obj (PyObject): The Python catalog object to register.
///     catalog_name (Optional[str]): The name to give the catalog. If None, a default name will be used.
///
/// Returns:
///     str: The name of the registered catalog (always "default" in the current implementation).
///
/// Raises:
///     PyErr: If there's an error during the registration process.
///
/// Example:
///     >>> import daft
///     >>> from my_python_catalog import MyPythonCatalog
///     >>> python_catalog = MyPythonCatalog()
///     >>> daft.register_python_catalog(python_catalog, "my_catalog")
///     'default'
#[pyfunction]
#[pyo3(name = "register_python_catalog")]
pub fn py_register_python_catalog(
    python_catalog_obj: PyObject,
    catalog_name: Option<&str>,
) -> PyResult<String> {
    let catalog = PythonCatalog::new(python_catalog_obj);
    daft_catalog::global_catalog::register_catalog(Arc::new(catalog), catalog_name);
    Ok("default".to_string())
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(py_register_python_catalog))?;
    Ok(())
}
