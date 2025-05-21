use std::sync::Arc;

use daft_core::prelude::SchemaRef;

use crate::{error::CatalogResult, Identifier, TableRef};

/// Catalog implementation reference.
pub type CatalogRef = Arc<dyn Catalog>;

/// A catalog provides object metadata such as namespaces, tables, and functions.
pub trait Catalog: Sync + Send + std::fmt::Debug {
    /// Returns the catalog name.
    fn name(&self) -> String;

    /// Create a namespace in the catalog, erroring if the namespace already exists.
    fn create_namespace(&mut self, ident: &Identifier) -> CatalogResult<()>;
    /// Check if a namespace exists in the catalog.
    fn has_namespace(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// Remove a namespace from the catalog, erroring if the namespace did not exist.
    fn drop_namespace(&mut self, ident: &Identifier) -> CatalogResult<()>;
    /// List all namespaces in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_namespaces(&self, prefix: Option<&Identifier>) -> CatalogResult<Vec<Identifier>>;

    /// Create a table in the catalog, erroring if the table already exists.
    fn create_table(&mut self, ident: &Identifier, schema: &SchemaRef) -> CatalogResult<TableRef>;
    /// Check if a table exists in the catalog.
    fn has_table(&self, ident: &Identifier) -> CatalogResult<bool>;
    /// Remove a table from the catalog, erroring if the table did not exist.
    fn drop_table(&mut self, ident: &Identifier) -> CatalogResult<()>;
    /// List all tables in the catalog. When a prefix is specified, list only nested namespaces with the prefix.
    fn list_tables(&self, prefix: Option<&Identifier>) -> CatalogResult<Vec<Identifier>>;
    /// Get a table from the catalog.
    fn get_table(&self, ident: &Identifier) -> CatalogResult<TableRef>;

    /// Leverage dynamic dispatch to return the inner object for a PyCatalogImpl (generics?)
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject>;
}
