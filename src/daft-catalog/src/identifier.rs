use std::fmt::Display;

use crate::error::{Error, Result};

/// A reference qualifier.
pub type Namespace = Vec<String>;

/// A reference to a catalog object.
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

    /// Returns a new simple identifier (no namespace)
    pub fn simple<T: Into<String>>(name: T) -> Self {
        Self {
            namespace: vec![],
            name: name.into(),
        }
    }

    /// Returns true if this is a qualified identifier.
    pub fn has_namespace(&self) -> bool {
        !self.namespace.is_empty()
    }

    /// Parses an identifier using sqlparser to validate the input.
    pub fn from_sql(input: &str, normalize: bool) -> Result<Identifier> {
        // TODO daft should define its own identifier domain.
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        let err = Error::InvalidIdentifier {
            input: input.to_string(),
        };
        let Ok(mut parser) = Parser::new(&PostgreSqlDialect {}).try_with_sql(input) else {
            return Err(err);
        };
        let Ok(idents) = parser.parse_multipart_identifier() else {
            return Err(err);
        };
        if idents.is_empty() {
            return Err(err);
        }
        // Convert sqlparser identifiers applying normalization if given.
        let mut names: Vec<String> = vec![];
        for ident in idents {
            if normalize && ident.quote_style.is_none() {
                names.push(ident.value.to_lowercase());
            } else {
                names.push(ident.value);
            }
        }
        let name = names.pop().unwrap();
        let namespace = names;
        Ok(Identifier::new(namespace, name))
    }
}

impl Display for Identifier {
    /// Returns the identifier as a dot-delimited string with double-quoted parts.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.namespace.is_empty() {
            write!(f, "{}", escape_double_quotes(&self.name))
        } else {
            let namespace = self
                .namespace
                .iter()
                .map(|n| escape_double_quotes(n))
                .collect::<Vec<String>>()
                .join(".");
            let name = escape_double_quotes(&self.name);
            write!(f, "{}.{}", namespace, name)
        }
    }
}

impl From<String> for Identifier {
    /// Returns an unqualified delimited identifier.
    fn from(name: String) -> Self {
        Self::simple(name)
    }
}

impl From<&str> for Identifier {
    /// Returns an unqualified delmited identifier.
    fn from(name: &str) -> Self {
        Self::simple(name.to_string())
    }
}

fn escape_double_quotes(text: &str) -> String {
    text.replace('"', "\"\"")
}
