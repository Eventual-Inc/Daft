//! APIs to handle Parquet <-> Arrow schemas.

use std::str::FromStr;

use crate::{
    datatypes::{Schema, TimeUnit},
    error::{Error, Result},
};

mod convert;
mod metadata;

pub use convert::parquet_to_arrow_schema_with_options;
pub(crate) use convert::*;
pub use metadata::{apply_schema_to_fields, read_schema_from_metadata};
pub use parquet2::{
    metadata::{FileMetaData, KeyValue, SchemaDescriptor},
    schema::types::ParquetType,
};
use serde::{Deserialize, Serialize};

use self::metadata::parse_key_value_metadata;

/// String encoding options.
///
/// Each variant of this enum maps to a different interpretation of the underlying binary data:
/// 1. `StringEncoding::Utf8` assumes the underlying binary data is UTF-8 encoded.
/// 2. `StringEncoding::Raw` makes no assumptions about the encoding of the underlying binary data. This variant will change the logical type of the column to `DataType::Binary` in the final schema.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StringEncoding {
    Raw,
    #[default]
    Utf8,
}

impl FromStr for StringEncoding {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "utf-8" => Ok(Self::Utf8),
            "raw" => Ok(Self::Raw),
            encoding => Err(Error::InvalidArgumentError(format!(
                "Unrecognized encoding: {}",
                encoding,
            ))),
        }
    }
}

/// Options when inferring schemas from Parquet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaInferenceOptions {
    /// When inferring schemas from the Parquet INT96 timestamp type, this is the corresponding TimeUnit
    /// in the inferred Arrow Timestamp type.
    ///
    /// This defaults to `TimeUnit::Nanosecond`, but INT96 timestamps outside of the range of years 1678-2262,
    /// will overflow when parsed as `Timestamp(TimeUnit::Nanosecond)`. Setting this to a lower resolution
    /// (e.g. TimeUnit::Milliseconds) will result in loss of precision, but support a larger range of dates
    /// without overflowing when parsing the data.
    pub int96_coerce_to_timeunit: TimeUnit,

    /// The string encoding to assume when inferring the schema from Parquet data.
    pub string_encoding: StringEncoding,
}

impl Default for SchemaInferenceOptions {
    fn default() -> Self {
        SchemaInferenceOptions {
            int96_coerce_to_timeunit: TimeUnit::Nanosecond,
            string_encoding: StringEncoding::default(),
        }
    }
}

/// Infers a [`Schema`] from parquet's [`FileMetaData`]. This first looks for the metadata key
/// `"ARROW:schema"`; if it does not exist, it converts the parquet types declared in the
/// file's parquet schema to Arrow's equivalent.
/// # Error
/// This function errors iff the key `"ARROW:schema"` exists but is not correctly encoded,
/// indicating that that the file's arrow metadata was incorrectly written.
pub fn infer_schema(file_metadata: &FileMetaData) -> Result<Schema> {
    infer_schema_with_options(file_metadata, None)
}

/// Like [`infer_schema`] but with configurable options which affects the behavior of inference
pub fn infer_schema_with_options(
    file_metadata: &FileMetaData,
    options: Option<SchemaInferenceOptions>,
) -> Result<Schema> {
    let mut metadata = parse_key_value_metadata(file_metadata.key_value_metadata());
    let fields = parquet_to_arrow_schema_with_options(file_metadata.schema().fields(), options);

    // Use arrow schema from metadata to apply Arrow-specific transformations on the inferred fields
    let schema = read_schema_from_metadata(&mut metadata)?;
    let transformed_fields = match schema {
        None => fields,
        Some(schema) => apply_schema_to_fields(&schema, &fields),
    };

    Ok(Schema {
        fields: transformed_fields,
        metadata,
    })
}
