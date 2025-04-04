use daft_catalog::FindMode;

// TODO make env variables
pub(crate) const _DAFT_SESSION: &str = "default";
pub(crate) const _DAFT_SESSION_USER: &str = "daft";
pub(crate) const _DAFT_SESSION_TEMP_DIR: &str = "/tmp";

/// Session state variables.
#[derive(Debug, Default)]
pub(crate) struct Options {
    pub identifier_mode: IdentifierMode,
    pub curr_catalog: Option<String>,
    pub curr_namespace: Option<Vec<String>>,
}

/// Identifier mode controls identifier resolution and name binding logic (tables, columns, views, etc).
#[derive(Debug, Default)]
#[allow(dead_code)]
pub enum IdentifierMode {
    /// Find case-insensitive (rvalue) and bind case-preserved (lvalue).
    Insensitive,
    /// Find case-sensitive (rvalue) and bind case-preserved (lvalue).
    #[default]
    Sensitive,
    /// Find case-sensitive (rvalue) and bind (lower) case-normalized (lvalue).
    Normalize,
}

/// Options helpers to convert session
impl Options {
    /// Returns the `FindMode` for the current `IdentifierMode`.
    pub fn find_mode(&self) -> FindMode {
        match self.identifier_mode {
            IdentifierMode::Insensitive => FindMode::Insensitive,
            IdentifierMode::Sensitive => FindMode::Sensitive,
            IdentifierMode::Normalize => FindMode::Sensitive,
        }
    }
}
