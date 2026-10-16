use daft_catalog::LookupMode;

// TODO make env variables
pub(crate) const _DAFT_SESSION: &str = "default";
pub(crate) const _DAFT_SESSION_USER: &str = "daft";
pub(crate) const _DAFT_SESSION_TEMP_DIR: &str = "/tmp";

/// Session state variables.
#[derive(Debug, Default, Clone)]
pub(crate) struct Options {
    pub identifier_mode: IdentifierMode,
    pub curr_catalog: Option<String>,
    pub curr_namespace: Option<Vec<String>>,
    pub curr_provider: Option<String>,
    pub curr_model: Option<String>,
}

/// Identifier mode controls identifier resolution and name binding logic (tables, columns, views, etc).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierMode {
    /// For `ident AS alias` -> lookup 'ident' case-insensitively and bind to 'alias' case-preserved.
    Insensitive,
    /// For `ident AS alias` -> lookup 'ident' case-sensitively and bind to 'alias' case-preserved.
    #[default]
    Sensitive,
    /// For `ident AS aLiAs` -> lookup 'ident' case-sensitively and bind to `lowercase('aLiAs') -> 'alias'`.
    Normalize,
}

impl std::str::FromStr for IdentifierMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sensitive" => Ok(Self::Sensitive),
            "insensitive" => Ok(Self::Insensitive),
            "normalize" | "normalized" => Ok(Self::Normalize),
            other => Err(format!(
                "identifier_mode '{other}', expected sensitive, insensitive, or normalize"
            )),
        }
    }
}

/// Options helpers to convert session
impl Options {
    /// Returns the binding `LookupMode` for the current `IdentifierMode`.
    pub fn lookup_mode(&self) -> LookupMode {
        match self.identifier_mode {
            IdentifierMode::Insensitive => LookupMode::Insensitive,
            IdentifierMode::Sensitive => LookupMode::Sensitive,
            IdentifierMode::Normalize => LookupMode::Sensitive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IdentifierMode;

    #[test]
    fn parses_identifier_mode_values() {
        assert_eq!(
            "sensitive".parse::<IdentifierMode>().unwrap(),
            IdentifierMode::Sensitive
        );
        assert_eq!(
            "insensitive".parse::<IdentifierMode>().unwrap(),
            IdentifierMode::Insensitive
        );
        assert_eq!(
            "normalize".parse::<IdentifierMode>().unwrap(),
            IdentifierMode::Normalize
        );
        assert_eq!(
            "normalized".parse::<IdentifierMode>().unwrap(),
            IdentifierMode::Normalize
        );
        assert_eq!(
            " NORMALIZE ".parse::<IdentifierMode>().unwrap(),
            IdentifierMode::Normalize
        );
        let err = "bogus".parse::<IdentifierMode>().unwrap_err();
        assert!(err.contains("bogus"), "unexpected error: {err}");
        assert!(err.contains("sensitive"), "unexpected error: {err}");
    }
}
