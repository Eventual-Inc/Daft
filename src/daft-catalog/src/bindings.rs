use std::collections::{HashMap, HashSet};

/// Bindings are stored case-normalized with the case-preserved bindings.
#[derive(Debug)]
pub struct Bindings<T> {
    /// case-preserved bindings (default)
    bindings: HashMap<String, T>,
    /// case-normalized binding aliases for insensitive lookups.
    aliases: HashMap<String, HashSet<String>>,
}

/// Bind mode is for lvalue handling which is currently out of scope.
#[derive(Debug, Clone, Copy)]
pub enum BindMode {
    Preserve,
    Normalize,
}

/// Find mode is for rvalue handling.
#[derive(Debug, Clone, Copy)]
pub enum FindMode {
    Insensitive,
    Sensitive,
}

/// Bindings implements "bind" and "find" for handling various identifier modes.
impl<T> Bindings<T> {
    /// Creates an empty bindings object.
    pub fn empty() -> Self {
        Self {
            bindings: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    /// Bind an object to the name (lvalue), overwriting any existing binding.
    pub fn bind(&mut self, name: String, object: T) {
        self.bindings.insert(name.clone(), object);
        self.aliases.entry(alias(&name)).or_default().insert(name);
    }

    /// Find all object by name (rvalue).
    pub fn find(&self, name: &str, mode: FindMode) -> Vec<&T> {
        match mode {
            FindMode::Insensitive => self.find_aliased(name),
            FindMode::Sensitive => self.find_binding(name),
        }
    }
    /// Finds this binding by exact case.
    #[inline]
    fn find_binding(&self, name: &str) -> Vec<&T> {
        self.bindings
            .get(name)
            .map(|binding| vec![binding])
            .unwrap_or_default()
    }

    /// Finds all bindings mapped to this alias.
    #[inline]
    fn find_aliased(&self, name: &str) -> Vec<&T> {
        self.aliases
            .get(&alias(name))
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| self.bindings.get(name))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Removes the binding if it exists (exact-case).
    pub fn remove(&mut self, name: &str) {
        self.bindings.remove(name);
    }

    /// Returns true if the binding exists (exact-case).
    pub fn contains(&self, name: &str) -> bool {
        self.bindings.contains_key(name)
    }

    /// List all objects matching the pattern.
    pub fn list(&self, pattern: Option<&str>) -> Vec<String> {
        self.bindings
            .keys()
            .map(|k| k.as_str())
            .filter(|k| pattern.is_none() || k.contains(pattern.unwrap_or("")))
            .map(|k| k.to_string())
            .collect()
    }

    /// Returns true iff there are no bindings.
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

/// Using the unicode lowercase to normalize
fn alias(name: &str) -> String {
    name.to_lowercase()
}
