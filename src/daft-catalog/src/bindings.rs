use std::collections::{HashMap, HashSet};

/// Bindings is used to bind names and lookup identifiers with various case-handling modes.
///
/// **Definitions**
/// * `object` - any 'thing' .. an 'x object' is a component of, or is otherwise associated with, some x, and cannot exist independently of that x (SQL)
/// * `name` - a name is some label (lvalue) associated with a reference-able object via "binding"
/// * `identifier` - an identifier is a reference (rvalue) to some named object which is resolved by looking up in the scope's bindings
/// * `bind` - to associate a name to an object
/// * `lookup` - to find object(s) from an identifier
/// * `identify` - to properly reference something without ambiguity (SQL)
///
/// (see also SQL-99 Part 1, page 5)
///
/// **Example**
///
/// ```sql
/// SELECT col AS alias FROM T;
/// -- 'col' is an identifier (rvalue) to reference a column.
/// -- 'alias' is a name (lvalue) to bind the resolved reference to.
/// -- 'T' is an identifier (rvalue) to reference a table.
/// ```
#[derive(Debug, Clone, Default)]
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

/// Lookup mode is for rvalue handling.
#[derive(Debug, Clone, Copy)]
pub enum LookupMode {
    Insensitive,
    Sensitive,
}

/// Bindings implements "bind" and "lookup" for handling various identifier modes.
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

    /// Lookup all object by name (rvalue).
    pub fn lookup(&self, name: &str, mode: LookupMode) -> Vec<&T> {
        match mode {
            LookupMode::Insensitive => self.lookup_bindings_by_alias(name),
            LookupMode::Sensitive => self.lookup_bindings(name),
        }
    }

    /// Lookup all matching bindings by exact case.
    fn lookup_bindings(&self, name: &str) -> Vec<&T> {
        self.bindings
            .get(name)
            .map(|binding| vec![binding])
            .unwrap_or_default()
    }

    /// Lookup all matching bindings using the the case-normalized alias.
    fn lookup_bindings_by_alias(&self, name: &str) -> Vec<&T> {
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
