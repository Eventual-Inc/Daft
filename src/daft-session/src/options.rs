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
#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub enum IdentifierMode {
    /// For `ident AS alias` -> lookup 'ident' case-insensitively and bind to 'alias' case-preserved.
    Insensitive,
    /// For `ident AS alias` -> lookup 'ident' case-sensitively and bind to 'alias' case-preserved.
    #[default]
    Sensitive,
    /// For `ident AS aLiAs` -> lookup 'ident' case-sensitively and bind to `lowercase('aLiAs') -> 'alias'`.
    Normalize,
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
