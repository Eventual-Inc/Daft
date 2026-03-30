use std::sync::Arc;

use daft_core::prelude::SchemaRef;

use crate::{Identifier, TableRef, error::CatalogResult};

/// A catalog function reference — an opaque Python object wrapped for Rust.
#[cfg(feature = "python")]
pub type CatalogFunctionRef = pyo3::Py<pyo3::PyAny>;

/// Catalog implementation reference.
pub type CatalogRef = Arc<dyn Catalog>;

/// A catalog provides object metadata such as namespaces, tables, and functions.
#[cfg(debug_assertions)]
pub trait Catalog: Sync + Send + std::fmt::Debug {
    /// Returns the catalog name.
    fn name(&self) -> String;

    /// Create a namespace in the catalog, erroring if the namespace already exists.
    fn create_namespace(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Create a table in the catalog, erroring if the table already exists.
    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef>;
    /// Remove a namespace from the catalog, erroring if the namespace did not exist.
    fn drop_namespace(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Remove a table from the catalog, erroring if the table did not exist.
    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Get a table from the catalog.
    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef>;
    /// Check if a namespace exists in the catalog.
    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// Check if a table exists in the catalog.
    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// List all namespaces in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_namespaces(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>>;
    /// List all tables in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>>;

    /// Get a function from the catalog by identifier.
    /// The identifier's last part is the function name, and preceding parts form the namespace.
    /// Returns None if the catalog does not support function lookup or the function does not exist.
    #[cfg(feature = "python")]
    fn get_function(&self, _ident: &Identifier) -> CatalogResult<Option<CatalogFunctionRef>> {
        Ok(None)
    }

    /// Create/extract a Python object that subclasses the Catalog ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

#[cfg(not(debug_assertions))]
pub trait Catalog: Sync + Send {
    /// Returns the catalog name.
    fn name(&self) -> String;

    /// Create a namespace in the catalog, erroring if the namespace already exists.
    fn create_namespace(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Create a table in the catalog, erroring if the table already exists.
    fn create_table(&self, ident: &Identifier, schema: SchemaRef) -> CatalogResult<TableRef>;
    /// Remove a namespace from the catalog, erroring if the namespace did not exist.
    fn drop_namespace(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Remove a table from the catalog, erroring if the table did not exist.
    fn drop_table(&self, ident: &Identifier) -> CatalogResult<()>;
    /// Get a table from the catalog.
    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef>;
    /// Check if a namespace exists in the catalog.
    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// Check if a table exists in the catalog.
    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// List all namespaces in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_namespaces(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>>;
    /// List all tables in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_tables(&self, pattern: Option<&str>) -> CatalogResult<Vec<Identifier>>;

    /// Get a function from the catalog by identifier.
    /// The identifier's last part is the function name, and preceding parts form the namespace.
    /// Returns None if the catalog does not support function lookup or the function does not exist.
    #[cfg(feature = "python")]
    fn get_function(&self, _ident: &Identifier) -> CatalogResult<Option<CatalogFunctionRef>> {
        Ok(None)
    }

    /// Create/extract a Python object that subclasses the Catalog ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}
