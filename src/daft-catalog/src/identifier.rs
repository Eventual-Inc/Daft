use std::fmt::Display;

use crate::errors::{Error, Result};

/// An object's namespace (location).
pub type Namespace = Vec<String>;

/// An object's name.
pub type Name = String;

/// A reference (path) to some catalog object.
#[derive(Debug, Clone)]
pub struct Identifier {
    namespace: Namespace,
    name: Name,
}

impl Identifier {
    /// Creates an identifier from its namespace and name.
    pub fn new(namespace: Namespace, name: Name) -> Self {
        Self { namespace, name }
    }

    /// Returns the namespace
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Returns the name
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// Returns true if this is a qualified identifier e.g. has a namespace.
    pub fn has_namespace(&self) -> bool {
        !self.namespace().is_empty()
    }

    /// Parses an identifier using sqlparser to validate the input.
    pub fn parse(input: &str) -> Result<Identifier> {
        // TODO daft should define its own identifier domain.
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        let err = Error::InvalidIdentifier {
            input: input.to_string(),
        };
        let mut parser = match Parser::new(&PostgreSqlDialect {}).try_with_sql(input) {
            Ok(res) => res,
            Err(_) => return Err(err),
        };
        let parts = match parser.parse_identifiers() {
            Ok(res) => res,
            Err(_) => return Err(err),
        };
        // TODO Identifier normalization is omitted until further discussion.
        let mut parts = parts
            .iter()
            .map(|part| part.value.to_string())
            .collect::<Vec<String>>();
        let name = parts.pop().unwrap();
        let namespace = parts;
        Ok(Identifier::new(namespace, name))
    }
}

impl Display for Identifier {
    /// Joins the identifier to a dot-delimited path.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.namespace().is_empty() {
            f.write_str(&self.name)
        } else {
            let prefix = self.namespace.join(".");
            let string = format!("{}.{}", prefix, self.name);
            f.write_str(&string)
        }
    }
}
