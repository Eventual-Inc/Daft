use std::collections::HashMap;

/// Bindings
///
/// Notes:
///  - Currently using Clone because all references are arcs.
///  - This is intentionally lightweight and everything is exact-case.
///  - All APIs are non-fallible because callers determine what is an error.
///  - It does not necessarily have map semantics.
///  - Callers are responsible for case-normalization hence String, &str.
///  - Intentionally using String and &str rather than Into and AsRef.
#[derive(Debug)]
pub struct Bindings<T>(HashMap<String, T>);

impl<T> Bindings<T> {
    /// Creates an empty catalog provider.
    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    /// Inserts a new binding with ownership.
    pub fn insert(&mut self, name: String, object: T) {
        self.0.insert(name, object);
    }

    /// Removes the binding if it exists.
    pub fn remove(&mut self, name: &str) {
        self.0.remove(name);
    }

    /// Returns true if the binding exists.
    pub fn exists(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Get an object reference by name.
    pub fn get(&self, name: &str) -> Option<&T> {
        self.0.get(name)
    }

    /// List all objects matching the pattern.
    pub fn list(&self, pattern: Option<&str>) -> Vec<String> {
        self.0
            .keys()
            .map(|k| k.as_str())
            .filter(|k| pattern.is_none() || k.contains(pattern.unwrap_or("")))
            .map(|k| k.to_string())
            .collect()
    }

    /// Returns true iff there are no bindings.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
