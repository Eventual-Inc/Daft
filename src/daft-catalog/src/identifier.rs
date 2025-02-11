use std::{fmt::Display, iter::once, vec::IntoIter};

use crate::error::{Error, Result};

/// A reference qualifier to a catalog object.
pub type Qualifier = Vec<String>;

/// A reference to a catalog object (rvalue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub qualifier: Qualifier,
    pub name: String,
}

impl Identifier {
    /// Creates an identifier from its qualifier and name.
    pub fn new(qualifier: Qualifier, name: String) -> Self {
        Self { qualifier, name }
    }

    /// Returns true if this is a qualified identifier.
    pub fn has_qualifier(&self) -> bool {
        !self.qualifier.is_empty()
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
        let qualifier = parts;
        Ok(Identifier::new(qualifier, name))
    }
}

impl Display for Identifier {
    /// Joins the identifier to a dot-delimited path.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.qualifier.is_empty() {
            f.write_str(&self.name)
        } else {
            let prefix = self.qualifier.join(".");
            let string = format!("{}.{}", prefix, self.name);
            f.write_str(&string)
        }
    }
}

impl From<String> for Identifier {
    // TODO input validation
    fn from(name: String) -> Self {
        Self {
            qualifier: vec![],
            name,
        }
    }
}

impl From<&str> for Identifier {
    // TODO input validation
    fn from(name: &str) -> Self {
        Self {
            qualifier: vec![],
            name: name.to_string(),
        }
    }
}

impl IntoIterator for Identifier {
    type Item = String;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.qualifier
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
        self.qualifier
            .iter()
            .chain(once(&self.name))
            .collect::<Vec<&String>>()
            .into_iter()
    }
}
