#[cfg(feature = "python")]
pub mod python;
use std::hash::{Hash, Hasher};

use common_error::DaftError;
use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DaftFile {
    value: FileValue,
}

impl DaftFile {
    pub fn new_from_data(data: Vec<u8>) -> Self {
        Self {
            value: FileValue::Data(data),
        }
    }
    pub fn new_from_reference(reference: String, io_config: Option<IOConfig>) -> Self {
        Self {
            value: FileValue::Reference(reference, Box::new(io_config)),
        }
    }
}

impl std::fmt::Display for DaftFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.get_type() {
            DaftFileType::Reference => write!(f, "File({:?})", self.value),
            DaftFileType::Data => write!(f, "File({:?})", self.value),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FileValue {
    /// A reference to a file.
    Reference(String, Box<Option<IOConfig>>),
    /// In memory data.
    Data(Vec<u8>),
}

impl DaftFile {
    pub fn get_type(&self) -> DaftFileType {
        match self.value {
            FileValue::Reference(_, _) => DaftFileType::Reference,
            FileValue::Data(_) => DaftFileType::Data,
        }
    }
    pub fn get_value(&self) -> &FileValue {
        &self.value
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
            Self::Reference => 0u8.hash(state),
            Self::Data => 1u8.hash(state),
        }
    }
}

impl Hash for DaftFile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.value {
            FileValue::Reference(s, io_conf) => {
                s.hash(state);
                io_conf.hash(state);
            }
            FileValue::Data(d) => d.hash(state),
        }
    }
}
