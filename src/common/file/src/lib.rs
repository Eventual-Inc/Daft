#[cfg(feature = "python")]
pub mod python;
use std::{hash::Hash, sync::Arc};

use common_error::DaftError;
use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FileReference {
    /// A reference to a file.
    Reference(String, Option<Arc<IOConfig>>),
    /// In memory data.
    Data(Arc<Vec<u8>>),
}

impl FileReference {
    pub fn new_from_data(data: Vec<u8>) -> Self {
        Self::Data(Arc::new(data))
    }
    pub fn new_from_reference(reference: String, io_config: Option<IOConfig>) -> Self {
        Self::Reference(reference, io_config.map(Arc::new))
    }
}

impl std::fmt::Display for FileReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File({:?})", self)
    }
}

impl FileReference {
    pub fn get_type(&self) -> DaftFileType {
        match self {
            Self::Reference(_, _) => DaftFileType::Reference,
            Self::Data(_) => DaftFileType::Data,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DaftFileType {
    Reference = 0,
    Data = 1,
}

impl TryFrom<u8> for DaftFileType {
    type Error = DaftError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Reference),
            1 => Ok(Self::Data),
            _ => Err(DaftError::ValueError(format!(
                "Invalid DaftFileType value: {}",
                value
            ))),
        }
    }
}
