/// A namespace for a catalog object.
pub type Namespace = Vec<String>;

/// A name for a catalog object (lvalue).
pub struct Name {
    pub namespace: Namespace,
    pub name: String,
}

impl Name {
    pub fn has_namespace(&self) -> bool {
        !self.namespace.is_empty()
    }
}

impl std::fmt::Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.namespace.is_empty() {
            f.write_str(&self.name)
        } else {
            let prefix = self.namespace.join(".");
            let string = format!("{}.{}", prefix, self.name);
            f.write_str(&string)
        }
    }
}

impl From<String> for Name {
    fn from(name: String) -> Self {
        Self {
            namespace: vec![],
            name,
        }
    }
}

impl From<&str> for Name {
    fn from(name: &str) -> Self {
        Self::from(name.to_string())
    }
}
