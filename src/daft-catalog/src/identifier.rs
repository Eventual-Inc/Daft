use std::fmt::Display;

use crate::error::{Error, Result};

/// A reference qualifier.
pub type Namespace = Vec<Name>;

/// A reference (or name) to a catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub namespace: Namespace,
    pub name: Name,
}

/// A name is an identifier part e.g. "simple identifier".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    pub text: String,
    pub regular: bool,
}

impl Name {
    /// Create a new simple identifier.
    pub fn new(text: String, regular: bool) -> Self {
        Self { text, regular }
    }

    /// Create a new regular simple identifier.
    pub fn regular<T: Into<String>>(text: T) -> Self {
        Self {
            text: text.into(),
            regular: true,
        }
    }

    /// Create a new delimited simple identifier.
    pub fn delimited<T: Into<String>>(text: T) -> Self {
        Self {
            text: text.into(),
            regular: false,
        }
    }
}

impl Identifier {
    /// Creates an identifier from its namespace and name.
    pub fn new(namespace: Namespace, name: Name) -> Self {
        Self { namespace, name }
    }

    /// Creates a regular identifier with no namespace (no input validation).
    pub fn regular<T: Into<String>>(text: T) -> Self {
        Self {
            namespace: vec![],
            name: Name::regular(text),
        }
    }

    /// Creates a delimited identifier with no namespace (no input validation).
    pub fn delimited<T: Into<String>>(text: T) -> Self {
        Self {
            namespace: vec![],
            name: Name::delimited(text),
        }
    }

    /// Returns true if this is a qualified identifier.
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
        let Ok(idents) = parser.parse_multipart_identifier() else {
            return Err(err);
        };
        if idents.is_empty() {
            return Err(err);
        }
        let mut names = idents.into_iter().map(|i| i.into()).collect::<Vec<Name>>();
        let name = names.pop().unwrap();
        let namespace = names;
        Ok(Identifier::new(namespace, name))
    }
}

impl Display for Name {
    /// Returns the ident as a string, adding double-quotes if delimited.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.regular {
            f.write_str(&self.text)
        } else {
            f.write_str(&format!("\"{}\"", &self.text))
        }
    }
}

impl Display for Identifier {
    /// Returns the identifier as a dot-delimited string.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.namespace.is_empty() {
            write!(f, "{}", &self.name)
        } else {
            let namespace = self
                .namespace
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<String>>()
                .join(".");
            let name = self.name.to_string();
            write!(f, "{}.{}", namespace, name)
        }
    }
}

impl From<sqlparser::ast::Ident> for Name {
    /// Returns a simple identifier from a sqlparser::Identifier
    fn from(ident: sqlparser::ast::Ident) -> Self {
        Self {
            text: ident.value,
            regular: ident.quote_style.is_none(),
        }
    }
}

impl From<String> for Name {
    /// Returns a delimited simple identifier.
    fn from(text: String) -> Self {
        Self {
            text,
            regular: false,
        }
    }
}

impl From<String> for Identifier {
    /// Returns an unqualified delimited identifier.
    fn from(text: String) -> Self {
        Self {
            namespace: vec![],
            name: text.into(),
        }
    }
}

impl From<Name> for Identifier {
    /// Returns an unqualified identifier.
    fn from(name: Name) -> Self {
        Self {
            namespace: vec![],
            name,
        }
    }
}
