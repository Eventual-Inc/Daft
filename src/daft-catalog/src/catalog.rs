use std::{collections::HashMap, sync::Arc};

use pyo3::ffi::PyObject;

use crate::{error::Result, Identifier, Table};

/// Catalogs is a collection of referenceable catalogs (glorified map).
///
/// Notes:
///  - This is intentionally lightweight and everything is exact-case.
///  - All APIs are non-fallible because callers determine what is an error.
///  - It does not necessarily have map semantics.
///  - Callers are responsible for case-normalization hence String, &str.
///  - Intentionally using String and &str rather than Into and AsRef.
#[derive(Debug)]
pub struct Catalogs(HashMap<String, Arc<dyn Catalog>>);

impl Catalogs {
    /// Creates an empty catalogs collection
    pub fn empty() -> Catalogs {
        Self(HashMap::new())
    }

    /// Attaches a catalog to this catalog collection.
    pub fn attach(&mut self, name: String, catalog: Arc<dyn Catalog>) {
        self.0.insert(name, catalog);
    }

    /// Detaches a catalog from this catalog collection.
    pub fn detach(&mut self, name: &str) {
        self.0.remove(name);
    }

    /// Returns true iff a catalog with the given name exists (exact-case).
    pub fn exists(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Get the catalog by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        self.0.get(name).map(Arc::clone)
    }

    /// Lists the catalogs
    pub fn list(&self, pattern: Option<&str>) -> Vec<String> {
        self.0
            .keys()
            .map(|k| k.as_str())
            .filter(|k| pattern.is_none() || k.contains(pattern.unwrap_or("")))
            .map(|k| k.to_string())
            .collect()
    }

    /// Returns true iff there are no catalogs in this collection.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A catalog provides object metadata such as namespaces, tables, and functions.
pub trait Catalog: Sync + Send + std::fmt::Debug {
    /// Returns the catalog name.
    fn name(&self) -> String;

    /// Returns the given table if it exists.
    fn get_table(&self, name: &Identifier) -> Result<Option<Box<dyn Table>>>;

    /// Leverage dynamic dispatch to return the inner object for a PyObjectImpl (use generics?).
    #[cfg(feature = "python")]
    fn to_py(&self, _: pyo3::Python<'_>) -> pyo3::PyObject {
        panic!("missing to_py implementation; consider PyCatalog(self) as the blanket implementation")
    }
}
