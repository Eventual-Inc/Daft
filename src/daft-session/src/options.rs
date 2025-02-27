// TODO make env variables
pub(crate) const _DAFT_SESSION: &str = "default";
pub(crate) const _DAFT_SESSION_USER: &str = "daft";
pub(crate) const _DAFT_SESSION_TEMP_DIR: &str = "/tmp";

#[derive(Debug, Default)]
pub(crate) struct Options {
    pub curr_catalog: Option<String>,
    pub curr_namespace: Option<Vec<String>>,
}
