use std::fmt::Display;

use crate::error::{CatalogError, CatalogResult};

/// A reference to a catalog object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier(Vec<String>);

/// Constructors should maintain the 'at least on part' invariant
impl Identifier {
    /// Creates a new identifier from an iterator of path parts, returning a Result.
    pub fn new<P, S>(path: P) -> Self
    where
        P: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::try_new(path).unwrap()
    }

    /// Creates a new identifier from an iterator of path parts, returning a Result.
    pub fn try_new<P, S>(path: P) -> CatalogResult<Self>
    where
        P: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let path: Vec<String> = path.into_iter().map(Into::into).collect();
        if path.is_empty() {
            return Err(CatalogError::invalid_identifier(
                "try_new received zero parts",
            ));
        }
        Ok(Self(path))
    }

    /// Returns a new simple identifier (no qualifier).
    pub fn simple<T: Into<String>>(name: T) -> Self {
        Self(vec![name.into()])
    }

    /// Returns a new qualified identifier.
    pub fn qualified<Q, N>(qualifier: Q, name: N) -> Self
    where
        Q: IntoIterator<Item = N>,
        N: Into<String>,
    {
        let mut path = vec![];
        path.extend(qualifier.into_iter().map(Into::into));
        path.push(name.into());
        Self(path)
    }

    /// Returns the part at index `idx`, same as like `[idx]`
    pub fn get(&self, idx: usize) -> &str {
        &self.0[idx]
    }

    /// Returns the length of the identifier, always >= 1
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the 'name' which is the last part of the identifier.
    pub fn name(&self) -> &str {
        self.0.last().expect("identifier should never be empty")
    }

    /// Returns the 'qualifier' which is everything before the 'name' in the identifier.
    pub fn qualifier(&self) -> Option<&[String]> {
        if self.0.len() <= 1 {
            None
        } else {
            Some(&self.0[0..self.0.len() - 1])
        }
    }

    /// Returns a Vec<String> of path parts, taking ownership of the identifier.
    pub fn path(self) -> Vec<String> {
        self.0
    }

    /// Returns a new Identifier with the qualifier prepended
    pub fn qualify<Q, N>(&self, qualifier: Q) -> Self
    where
        Q: IntoIterator<Item = N>,
        N: Into<String>,
    {
        let mut path: Vec<String> = qualifier.into_iter().map(Into::into).collect();
        path.extend_from_slice(&self.0[..]);
        Self(path)
    }

    /// Returns a new Identifier with the first n elements of the qualifier removed
    pub fn drop(&self, n: usize) -> Self {
        let skip = n.min(self.0.len().saturating_sub(1));
        Self(self.0[skip..].to_vec())
    }

    /// Returns true if this is a qualified identifier.
    pub fn has_qualifier(&self) -> bool {
        self.len() > 1
    }

    /// Parses an identifier using sqlparser to validate the input.
    pub fn from_sql(input: &str, normalize: bool) -> CatalogResult<Identifier> {
        // TODO daft should define its own identifier domain.
        use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        let err = CatalogError::InvalidIdentifier {
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
        let mut path: Vec<String> = vec![];
        for ident in idents {
            if normalize && ident.quote_style.is_none() {
                path.push(ident.value.to_lowercase());
            } else {
                path.push(ident.value);
            }
        }
        Ok(Self(path))
    }
}

impl Display for Identifier {
    /// Returns the identifier as a dot-delimited string with double-quoted parts.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = self
            .0
            .iter()
            .map(|n| escape_double_quotes(n))
            .collect::<Vec<String>>()
            .join(".");
        f.write_str(&parts)
    }
}

impl From<String> for Identifier {
    /// Returns an unqualified identifier.
    fn from(name: String) -> Self {
        Self::simple(name)
    }
}

impl From<&str> for Identifier {
    /// Returns an unqualified identifier.
    fn from(name: &str) -> Self {
        Self::simple(name.to_string())
    }
}

impl From<Vec<&str>> for Identifier {
    /// Returns an identifier from vec literal like vec!["a", "b"].into()
    fn from(path: Vec<&str>) -> Self {
        Self::new(path)
    }
}

fn escape_double_quotes(text: &str) -> String {
    text.replace('"', "\"\"")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_qualify() -> CatalogResult<()> {
        // single part
        let id = Identifier::simple("name");
        let qualified = id.qualify(vec!["prefix".to_string()]);
        assert_eq!(qualified.0[0], "prefix");
        assert_eq!(qualified.0[1], "name");
        // multi part
        let id = Identifier::try_new(vec!["a", "b", "name"])?;
        let qualified = id.qualify(vec!["prefix".to_string()]);
        assert_eq!(qualified.0, vec!["prefix", "a", "b", "name"]);
        Ok(())
    }

    #[test]
    fn test_drop() -> CatalogResult<()> {
        let id = Identifier::try_new(vec!["a", "b", "c"])?;
        // drop first element
        let dropped = id.drop(1);
        assert_eq!(dropped.0, vec!["b", "c"]);
        // drop all elements except last
        let dropped = id.drop(2);
        assert_eq!(dropped.0, vec!["c"]);
        // drop more than exists
        let dropped = id.drop(10);
        assert_eq!(dropped.0, vec!["c"]);
        Ok(())
    }

    #[test]
    fn test_from_vec() -> CatalogResult<()> {
        // single part
        let id = Identifier::try_new(vec!["a"])?;
        assert_eq!(id.0, vec!["a"]);
        // multi part (2)
        let id = Identifier::try_new(vec!["a", "b"])?;
        assert_eq!(id.0, vec!["a", "b"]);
        // multi part (3)
        let id = Identifier::try_new(vec!["a", "b", "c"])?;
        assert_eq!(id.0, vec!["a", "b", "c"]);
        // err! empty vec
        let id = Identifier::try_new(vec![] as Vec<String>);
        assert!(matches!(id, Err(CatalogError::InvalidIdentifier { .. })));
        Ok(())
    }
}
