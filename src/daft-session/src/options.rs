// TODO make env variables
pub(crate) const _DAFT_SESSION: &str = "default";
pub(crate) const _DAFT_SESSION_USER: &str = "daft";
pub(crate) const _DAFT_SESSION_TEMP_DIR: &str = "/tmp";
pub(crate) const DAFT_SESSION_DEFAULT_CATALOG: &str = "daft";

#[derive(Debug)]
pub(crate) struct Options {
    pub curr_catalog: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            curr_catalog: DAFT_SESSION_DEFAULT_CATALOG.to_string(),
        }
    }
}
