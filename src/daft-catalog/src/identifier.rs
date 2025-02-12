use std::{fmt::Display, iter::once, vec::IntoIter};

use crate::error::{Error, Result};

/// An object's namespace (location).
pub type Namespace = Vec<String>;

/// A reference (path) to some catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub namespace: Namespace,
    pub name: String,
}

impl Identifier {
    /// Creates an identifier from its namespace and name.
    pub fn new(namespace: Namespace, name: String) -> Self {
        Self { namespace, name }
    }

    /// Returns true if this is a qualified identifier e.g. has a namespace.
    pub fn has_namespace(&self) -> bool {
        !self.namespace.is_empty()
    }

    /// Parses an identifier using sqlparser to validate the input.
    pub fn parse(input: &str) -> Result<Identifier> {
        // TODO daft should define its own identifier domain.
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        let err = Error::InvalidIdentifier {
            input: input.to_string(),
        };
        let Ok(mut parser) = Parser::new(&PostgreSqlDialect {}).try_with_sql(input) else {
            return Err(err);
        };
        let Ok(parts) = parser.parse_multipart_identifier() else {
            return Err(err);
        };
        if parts.is_empty() {
            return Err(err);
        }
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
        if self.namespace.is_empty() {
            f.write_str(&self.name)
        } else {
            let prefix = self.namespace.join(".");
            let string = format!("{}.{}", prefix, self.name);
            f.write_str(&string)
        }
    }
}

impl IntoIterator for Identifier {
    type Item = String;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.namespace
            .into_iter()
            .chain(once(self.name))
            .collect::<Vec<String>>()
            .into_iter()
    }
}

impl<'a> IntoIterator for &'a Identifier {
    type Item = &'a String;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.namespace
            .iter()
            .chain(once(&self.name))
            .collect::<Vec<&String>>()
            .into_iter()
    }
}
