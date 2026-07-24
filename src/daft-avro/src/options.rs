use std::{fmt, str::FromStr};

use common_py_serde::impl_bincode_py_state_serialization;
use serde::{Deserialize, Serialize};

use crate::AvroError;

/// Compression codecs supported for Avro files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvroCompression {
    Null,
    Deflate,
}

impl FromStr for AvroCompression {
    type Err = AvroError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "null" | "none" | "uncompressed" => Ok(Self::Null),
            "deflate" => Ok(Self::Deflate),
            _ => Err(AvroError::UnsupportedCodec {
                codec: s.to_string(),
            }),
        }
    }
}

impl fmt::Display for AvroCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Deflate => write!(f, "deflate"),
        }
    }
}

impl AvroCompression {}

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Configuration for reading Avro data sources.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash, Default)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", get_all, from_py_object)
)]
pub struct AvroSourceConfig {}

impl AvroSourceConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        vec![]
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl AvroSourceConfig {
    #[new]
    #[pyo3(signature = ())]
    fn new() -> Self {
        Self::default()
    }
}

impl_bincode_py_state_serialization!(AvroSourceConfig);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_from_str_null() {
        assert_eq!(
            "null".parse::<AvroCompression>().unwrap(),
            AvroCompression::Null
        );
        assert_eq!(
            "none".parse::<AvroCompression>().unwrap(),
            AvroCompression::Null
        );
        assert_eq!(
            "uncompressed".parse::<AvroCompression>().unwrap(),
            AvroCompression::Null
        );
        assert_eq!(
            "NULL".parse::<AvroCompression>().unwrap(),
            AvroCompression::Null
        );
    }

    #[test]
    fn test_compression_from_str_deflate() {
        assert_eq!(
            "deflate".parse::<AvroCompression>().unwrap(),
            AvroCompression::Deflate
        );
    }

    #[test]
    fn test_compression_from_str_invalid() {
        assert!("snappy".parse::<AvroCompression>().is_err());
        assert!("zstd".parse::<AvroCompression>().is_err());
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(AvroCompression::Null.to_string(), "null");
        assert_eq!(AvroCompression::Deflate.to_string(), "deflate");
    }
}

/// Options for writing Avro files.
#[derive(Clone, Debug)]
pub struct AvroWriteOptions {
    /// Compression codec.
    pub compression: AvroCompression,
    /// Top-level Avro record name.
    pub record_name: String,
    /// Avro record namespace.
    pub record_namespace: String,
}

impl Default for AvroWriteOptions {
    fn default() -> Self {
        Self {
            compression: AvroCompression::Null,
            record_name: "record".to_string(),
            record_namespace: String::new(),
        }
    }
}

impl AvroWriteOptions {
    #[must_use]
    pub fn new(
        compression: AvroCompression,
        record_name: String,
        record_namespace: String,
    ) -> Self {
        Self {
            compression,
            record_name,
            record_namespace,
        }
    }
}
