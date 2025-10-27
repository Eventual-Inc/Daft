#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "python",
    pyclass(name = "PyFileFormat", module = "daft.daft", eq, eq_int)
)]
pub enum MediaType {
    Unknown,
    Video,
}

#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pymethods)]
impl MediaType {
    #[staticmethod]
    pub fn unknown() -> Self {
        Self::Unknown
    }
    #[staticmethod]
    pub fn video() -> Self {
        Self::Video
    }
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Video => write!(f, "Video"),
        }
    }
}
