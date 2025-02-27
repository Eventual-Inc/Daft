use std::sync::Arc;

use crate::{bindings::Bindings, error::Result, Identifier, Table};

/// Catalog implementation reference.
pub type CatalogRef = Arc<dyn Catalog>;

/// CatalogProvider is a collection of referenceable catalogs.
pub type CatalogProvider = Bindings<CatalogRef>;

/// A catalog provides object metadata such as namespaces, tables, and functions.
pub trait Catalog: Sync + Send + std::fmt::Debug {
    /// Returns the catalog name.
    fn name(&self) -> String;

    /// Returns the given table if it exists.
    fn get_table(&self, ident: &Identifier) -> Result<Option<Box<dyn Table>>>;

    /// Leverage dynamic dispatch to return the inner object for a PyCatalogImpl (generics?)
    #[cfg(feature = "python")]
    fn to_py(&self, _: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject> {
        panic!(
            "missing to_py implementation, consider PyCatalog(self) as the blanket implementation"
        )
    }
}
