use std::sync::Arc;

use daft_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::{data_catalog::DataCatalog, GLOBAL_DAFT_META_CATALOG};

/// Registers an AWS Glue catalog instance with Daft
#[pyfunction]
#[pyo3(name = "register_aws_glue")]
pub fn register_aws_glue() -> PyResult<()> {
    todo!("Register an AWS Glue catalog");
}

/// Registers an Iceberg REST service with Daft
#[pyfunction]
#[pyo3(name = "register_iceberg_rest")]
pub fn register_iceberg_rest() -> PyResult<()> {
    todo!("Register an Iceberg REST catalog");
}

/// Registers a Hive Metastore (HMS) service with Daft
#[pyfunction]
#[pyo3(name = "register_hive_metastore")]
pub fn register_hive_metastore() -> PyResult<()> {
    todo!("Register a Hive Metastore catalog");
}

/// Registers a Unity Catalog instance with Daft
#[pyfunction]
#[pyo3(name = "register_unity_catalog")]
pub fn register_unity_catalog() -> PyResult<()> {
    todo!("Register a Unity Catalog");
}

/// Retrieves a catalog instance by name.
///
/// This function retrieves a DataCatalog instance by name.
///
/// If no name is provided, it returns the default catalog.
///
/// Args:
///     name (Optional[str]): The name of the catalog to retrieve. If None, the default catalog is returned.
///
/// Returns:
///     PyDataCatalog: A PyDataCatalog instance representing the requested catalog.
///
/// Raises:
///     DaftError: If the specified catalog is not found.
///
/// Example:
///     >>> import daft
///     >>> catalog = daft.get_catalog("my_catalog")
///     >>> default_catalog = daft.get_catalog()
#[pyfunction]
#[pyo3(name = "get_catalog")]
pub fn get_catalog(_name: Option<&str>) -> PyResult<PyDataCatalog> {
    todo!("Return a PyDataCatalog instance for the specified catalog name")
}

#[pyclass]
#[pyo3(name = "DataCatalog")]
pub struct PyDataCatalog {
    pub data_catalog: Arc<dyn DataCatalog>,
}

/// Read a table from the specified `DaftMetaCatalog`.
///
/// This function reads a table from a `DaftMetaCatalog` and returns a PyLogicalPlanBuilder
/// object representing the plan required to read the table.
///
/// The provided `table_identifier` can be:
///
/// 1. Name of a registered dataframe/SQL view (manually registered using `DaftMetaCatalog.register_view`)
/// 2. Name of a table within the default catalog (without inputting the catalog name)
/// 3. Name of a fully-qualitied table path ("<catalog_name>.<table_name>")
///
/// Args:
///     table_identifier (str): The identifier of the table to read.
///     catalog (PyDaftMetaCatalog): The catalog instance from which to read the table.
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
fn py_read_table(
    table_identifier: &str,
    catalog_name: Option<&str>,
) -> PyResult<PyLogicalPlanBuilder> {
    let logical_plan_builder =
        GLOBAL_DAFT_META_CATALOG.read_table(table_identifier, catalog_name)?;
    Ok(PyLogicalPlanBuilder::new(logical_plan_builder))
}

#[pymodule]
fn daft_catalog(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyDataCatalog>()?;
    m.add_wrapped(wrap_pyfunction!(py_read_table))?;
    m.add_wrapped(wrap_pyfunction!(get_catalog))?;
    Ok(())
}
