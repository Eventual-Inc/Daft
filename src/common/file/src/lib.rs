#[cfg(feature = "python")]
pub mod python;
use std::hash::{Hash, Hasher};

use common_error::DaftError;
use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DaftFile {
    file_type: DaftFileType,
    value: Value,
}

impl DaftFile {
    pub fn new_from_bytes(data: Vec<u8>) -> Self {
        Self {
            file_type: DaftFileType::Data,
            value: Value::Data(data),
        }
    }
    pub fn new_from_reference(reference: String, io_config: Option<IOConfig>) -> Self {
        Self {
            file_type: DaftFileType::Reference,
            value: Value::Reference(reference, io_config),
        }
    }
}

impl std::fmt::Display for DaftFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.file_type {
            DaftFileType::Reference => write!(f, "File({:?})", self.value),
            DaftFileType::Data => write!(f, "File({:?})", self.value),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Value {
    /// A reference to a file.
    Reference(String, Option<IOConfig>),
    /// In memory data.
    Data(Vec<u8>),
}

impl DaftFile {
    pub fn get_type(&self) -> DaftFileType {
        self.file_type.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
impl Hash for DaftFileType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DaftFileType::Reference => 0u8.hash(state),
            DaftFileType::Data => 1u8.hash(state),
        }
    }
}

impl Hash for DaftFile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_type.hash(state);
        match &self.value {
            Value::Reference(s, io_conf) => {
                s.hash(state);
                io_conf.hash(state);
            }
            Value::Data(d) => d.hash(state),
        }
    }
}
