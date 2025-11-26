use std::sync::Arc;

use daft_core::prelude::SchemaRef;

use crate::{Identifier, TableRef, error::CatalogResult};

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

    /// Create/extract a Python object that subclasses the Catalog ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}
