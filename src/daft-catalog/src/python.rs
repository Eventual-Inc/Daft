use daft_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::{read_table, DaftCatalog};

#[pyclass]
#[pyo3(name = "DaftCatalog")]
struct PyDaftCatalog {
    pub daft_catalog: DaftCatalog,
}

#[pymethods]
impl PyDaftCatalog {
    #[new]
    pub fn new() -> Self {
        Self {
            daft_catalog: DaftCatalog {},
        }
    }

    /// Registers an AWS Glue catalog instance with Daft
    pub fn register_aws_glue(&self) -> PyResult<Self> {
        todo!("Register an AWS Glue catalog");
    }

    /// Registers an Iceberg REST service with Daft
    pub fn register_iceberg_rest(&self) -> PyResult<Self> {
        todo!("Register an Iceberg REST catalog");
    }

    /// Registers a Hive Metastore (HMS) service with Daft
    pub fn register_hive_metastore(&self) -> PyResult<Self> {
        todo!("Register a Hive Metastore catalog");
    }

    /// Registers a Unity Catalog instance with Daft
    pub fn register_unity_catalog(&self) -> PyResult<Self> {
        todo!("Register a Unity Catalog");
    }

    /// List the names of all registered catalogs (an empty string indicates the default catalog)
    ///
    /// Returns:
    ///     A vector of strings containing the names of the registered catalogs.
    pub fn list_catalogs(&self) -> PyResult<Vec<String>> {
        todo!("Return a list of catalog names")
    }
}

/// Read a table from the specified `DaftCatalog`.
///
/// This function reads a table from a `DaftCatalog` and returns a PyLogicalPlanBuilder
/// object representing the plan required to read the table.
///
/// The provided `table_identifier` can be:
///
/// 1. Name of a registered dataframe/SQL view (manually registered using `DaftCatalog.register_view`)
/// 2. Name of a table within the default catalog (without inputting the catalog name)
/// 3. Name of a fully-qualitied table path ("<catalog_name>.<table_name>")
///
/// Args:
///     table_identifier (str): The identifier of the table to read.
///     catalog (PyDaftCatalog): The catalog instance from which to read the table.
///
/// Returns:
///     PyLogicalPlanBuilder: A PyLogicalPlanBuilder object representing the table's data.
///
/// Raises:
///     DaftError: If the table cannot be read or the specified table identifier is not found.
///
/// Example:
///     >>> import daft
///     >>> from daft import read_table, DaftCatalog
///     >>>
///     >>> catalog = DaftCatalog()
///     >>> catalog = catalog.register_view("foo", daft.from_pydict({"bar": [1, 2, 3]}))
///     >>>
///     >>> df = daft_catalog.read_table("foo", catalog)
#[pyfunction]
#[pyo3(name = "read_table")]
fn py_read_table(
    table_identifier: &str,
    catalog: &PyDaftCatalog,
) -> PyResult<PyLogicalPlanBuilder> {
    Ok(PyLogicalPlanBuilder::new(read_table(
        table_identifier,
        &catalog.daft_catalog,
    )?))
}

#[pymodule]
fn daft_catalog(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyDaftCatalog>()?;
    m.add_wrapped(wrap_pyfunction!(py_read_table))?;
    Ok(())
}
