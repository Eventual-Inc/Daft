use std::fmt::Display;

use crate::error::{Error, Result};

/// A reference qualifier.
pub type Qualifier = Vec<String>;

/// A reference to a catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub qualifier: Qualifier,
    pub name: String,
}

// TODO chore: simplify the Identifier abstraction to just Vec<String>.
impl Identifier {
    /// Creates an identifier from its namespace and name.
    pub fn new(qualifier: Qualifier, name: String) -> Self {
        Self { qualifier, name }
    }

    /// Returns a new simple identifier (no namespace)
    pub fn simple<T: Into<String>>(name: T) -> Self {
        Self {
            qualifier: vec![],
            name: name.into(),
        }
    }

    /// Returns a new Identifier with the qualifier prepended
    pub fn qualify(&self, qualifier: Qualifier) -> Self {
        let name = self.name.clone();
        let mut qualifier = qualifier;
        qualifier.extend_from_slice(&self.qualifier);
        Self::new(qualifier, name)
    }

    /// Returns a new Identifier with the first n elements of the qualifier removed
    pub fn drop(&self, n: usize) -> Self {
        let mut qualifier = self.qualifier.clone();
        qualifier.drain(..n.min(qualifier.len()));
        Self::new(qualifier, self.name.clone())
    }

    /// Returns true if this is a qualified identifier.
    pub fn has_qualifier(&self) -> bool {
        !self.qualifier.is_empty()
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

    /// This will replace `new` once Identifier is updated to just Vec<String>.
    pub fn from_path<I, T>(iter: I) -> Result<Identifier>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let parts: Vec<String> = iter.into_iter().map(Into::into).collect();
        if parts.is_empty() {
            return Err(Error::InvalidIdentifier {
                input: "".to_string(),
            });
        }
        let name = parts.last().unwrap().clone();
        let qualifier = parts[..parts.len() - 1].to_vec();
        Ok(Self::new(qualifier, name))
    }
}

impl Display for Identifier {
    /// Returns the identifier as a dot-delimited string with double-quoted parts.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.qualifier.is_empty() {
            write!(f, "{}", escape_double_quotes(&self.name))
        } else {
            let namespace = self
                .qualifier
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_qualify() -> Result<()> {
        // single part
        let id = Identifier::simple("name");
        let qualified = id.qualify(vec!["prefix".to_string()]);
        assert_eq!(qualified.qualifier, vec!["prefix"]);
        assert_eq!(qualified.name, "name");
        // multi part
        let id = Identifier::new(vec!["a".to_string(), "b".to_string()], "name".to_string());
        let qualified = id.qualify(vec!["prefix".to_string()]);
        assert_eq!(qualified.qualifier, vec!["prefix", "a", "b"]);
        assert_eq!(qualified.name, "name");
        Ok(())
    }

    #[test]
    fn test_drop() -> Result<()> {
        let id = Identifier::from_path(vec!["a", "b", "c"])?;
        // drop first element
        let dropped = id.drop(1);
        assert_eq!(dropped.qualifier, vec!["b"]);
        assert_eq!(dropped.name, "c");
        // drop all elements
        let dropped = id.drop(2);
        assert_eq!(dropped.qualifier, Vec::<String>::new());
        assert_eq!(dropped.name, "c");
        // drop more than exists
        let dropped = id.drop(10);
        assert_eq!(dropped.qualifier, Vec::<String>::new());
        assert_eq!(dropped.name, "c");
        Ok(())
    }

    #[test]
    fn test_from_vec() -> Result<()> {
        // single part
        let id = Identifier::from_path(vec!["a"])?;
        assert_eq!(id.qualifier, Vec::<String>::new());
        assert_eq!(id.name, "a");
        // multi part (2)
        let id = Identifier::from_path(vec!["a", "b"])?;
        assert_eq!(id.qualifier, vec!["a"]);
        assert_eq!(id.name, "b");
        // multi part (3)
        let id = Identifier::from_path(vec!["a", "b", "c"])?;
        assert_eq!(id.qualifier, vec!["a", "b"]);
        assert_eq!(id.name, "c");
        // err! empty vec
        let id = Identifier::from_path(vec![] as Vec<String>);
        assert!(matches!(id, Err(Error::InvalidIdentifier { .. })));
        Ok(())
    }
}
