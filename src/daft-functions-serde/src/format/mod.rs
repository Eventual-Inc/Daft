use std::{fmt::Display, str::FromStr};

use common_error::{value_err, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Utf8Array},
    series::Series,
};
use daft_dsl::{FromLiteral, Literal, LiteralValue};
use serde::{Deserialize, Serialize};

mod json;

/// Signature of a deserialization implementation e.g. strings to arbitrary series.
pub type Deserializer = fn(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series>;

/// Signature of a serialization implementation e.g. arbitrary series to string.
pub type Serializer = fn(input: Series) -> DaftResult<Utf8Array>;

/// Supported formsts for the serialize and deserialize functions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Format {
    Json,
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Json => "json",
        })
    }
}

impl Literal for Format {
    fn literal_value(self) -> daft_dsl::LiteralValue {
        LiteralValue::Utf8(self.to_string())
    }
}

impl FromLiteral for Format {
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
        if let LiteralValue::Utf8(s) = lit {
            s.parse()
        } else {
            value_err!("Expected a string literal, got {:?}", lit)
        }
    }
}

impl FromStr for Format {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            _ => Err(DaftError::not_implemented(format!(
                "unsupported format: {}",
                s
            ))),
        }
    }
}

impl Format {
    pub(crate) fn deserializer(&self) -> Deserializer {
        match self {
            Self::Json => json::deserialize,
        }
    }

    pub(crate) fn try_deserializer(&self) -> Deserializer {
        match self {
            Self::Json => json::try_deserialize,
        }
    }

    pub(crate) fn serializer(&self) -> Serializer {
        match self {
            Self::Json => json::serialize,
        }
    }

    pub(crate) fn try_serializer(&self) -> Serializer {
        match self {
            Self::Json => json::try_serialize,
        }
    }
}
