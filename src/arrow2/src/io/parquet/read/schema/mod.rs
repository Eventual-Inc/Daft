//! APIs to handle Parquet <-> Arrow schemas.

use crate::{
    datatypes::{Schema, TimeUnit},
    error::Result,
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

use self::metadata::parse_key_value_metadata;

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

    /// If `true`, when inferring schemas from the Parquet String (UTF8 encoded) type, always coerce to a binary type.
    /// This will avoid any UTF8 validation that would have originally been performed.
    pub string_coerce_to_binary: bool,
}

impl Default for SchemaInferenceOptions {
    fn default() -> Self {
        SchemaInferenceOptions {
            int96_coerce_to_timeunit: TimeUnit::Nanosecond,
            string_coerce_to_binary: false,
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
