use std::hash::{Hash, Hasher};

use common_error::DaftError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DaftFile {
    /// A reference to a file.
    Reference(String),
    /// In memory data.
    Data(Vec<u8>),
}

impl DaftFile {
    pub fn get_type(&self) -> DaftFileType {
        match self {
            Self::Reference(_) => DaftFileType::Reference,
            Self::Data(_) => DaftFileType::Data,
        }
    }
}

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

impl Eq for DaftFile {}

impl Hash for DaftFile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Reference(path) => {
                0u8.hash(state);
                path.hash(state);
            }
            Self::Data(bytes) => {
                1u8.hash(state);
                bytes.hash(state);
            }
        }
    }
}
