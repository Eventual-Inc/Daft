use std::{collections::HashMap, sync::Arc};

use crate::DataCatalog;

/// Catalogs is a collection of referenceable catalogs (glorified map).
///
/// Notes:
///  - This is intentionally lightweight and everything is exact-case.
///  - All APIs are non-fallible because callers determine what is an error.
///  - It does not necessarily have map semantics.
///  - Callers are responsible for case-normalization hence String, &str.
///  - Intentionally using String and &str rather than Into and AsRef.
#[derive(Debug)]
pub struct Catalogs(HashMap<String, Arc<dyn DataCatalog>>);

impl Catalogs {
    /// Creates an empty catalogs collection
    pub fn empty() -> Catalogs {
        Self(HashMap::new())
    }

    /// Attaches a catalog to this catalog collection.
    pub fn attach(&mut self, name: String, catalog: Arc<dyn DataCatalog>) {
        self.0.insert(name, catalog);
    }

    /// Detaches a catalog from this catalog collection.
    pub fn detach(&mut self, name: &str) {
        self.0.remove(name);
    }

    /// Get the catalog by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn DataCatalog>> {
        self.0.get(name).map(Arc::clone)
    }

    /// Returns true iff a catalog with the given name exists (exact-case).
    pub fn exists(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }
}

// TODO Catalog trait for implementations.
