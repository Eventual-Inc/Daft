use std::collections::BTreeMap;

use base64::{engine::general_purpose, Engine as _};
pub use parquet2::metadata::KeyValue;

use crate::datatypes::{DataType, Field, Metadata, Schema};
use crate::error::{Error, Result};
use crate::io::ipc::read::deserialize_schema;

use super::super::super::ARROW_SCHEMA_META_KEY;

/// Reads an arrow schema from Parquet's file metadata. Returns `None` if no schema was found.
/// # Errors
/// Errors iff the schema cannot be correctly parsed.
pub fn read_schema_from_metadata(metadata: &mut Metadata) -> Result<Option<Schema>> {
    metadata
        .remove(ARROW_SCHEMA_META_KEY)
        .map(|encoded| get_arrow_schema_from_metadata(&encoded))
        .transpose()
}

/// Try to convert Arrow schema metadata into a schema
fn get_arrow_schema_from_metadata(encoded_meta: &str) -> Result<Schema> {
    let decoded = general_purpose::STANDARD.decode(encoded_meta);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            deserialize_schema(slice).map(|x| x.0)
        }
        Err(err) => {
            // The C++ implementation returns an error if the schema can't be parsed.
            Err(Error::InvalidArgumentError(format!(
                "Unable to decode the encoded schema stored in {ARROW_SCHEMA_META_KEY}, {err:?}"
            )))
        }
    }
}

pub(super) fn parse_key_value_metadata(key_value_metadata: &Option<Vec<KeyValue>>) -> Metadata {
    key_value_metadata
        .as_ref()
        .map(|key_values| {
            key_values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|value| (kv.key.clone(), value.clone()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Applies a supplied `origin_field` to the `inferred_field` which was inferred from Parquet schema
///
/// This logic is adapted from a similar function in arrow-cpp
/// See: `ApplyOriginalMetadata` in https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/schema.cc#L976C14-L999
fn apply_original_metadata(origin_field: &Field, inferred_field: &Field) -> Field {
    let new_field = match (origin_field.data_type(), inferred_field.data_type()) {
        // Wrap inferred field into an Extension type
        (DataType::Extension(_, storage_dtype, _), _inferred_dtype) => {
            // Recursively apply the origin_field (coerced into its storage type) to the inferred_field
            let origin_storage_field = Field::new(
                origin_field.name.clone(),
                storage_dtype.as_ref().clone(),
                origin_field.is_nullable,
            );
            let recursively_applied_field = apply_original_metadata(&origin_storage_field, inferred_field);

            // Only return the Extension type field if the recursively applied field matches the Extension's storage type
            if recursively_applied_field.data_type() == storage_dtype.as_ref() {
                Field::new(
                    recursively_applied_field.name.clone(),
                    origin_field.data_type().clone(),
                    recursively_applied_field.is_nullable,
                )
            } else {
                recursively_applied_field
            }
        },

        // If inferred timestamp field is timezone-aware (inferred as "+00:00" by Arrow2),
        // then set original time zone since Parquet has no native storage for timezones
        (
            DataType::Timestamp(_, Some(origin_tz)),
            DataType::Timestamp(inferred_tu, Some(inferred_tz))
        ) if inferred_tz.eq("+00:00") => {
            Field::new(
                inferred_field.name.clone(),
                DataType::Timestamp(*inferred_tu, Some(origin_tz.clone())),
                inferred_field.is_nullable,
            )
        }

        // Apply DataType::Duration on Int64 fields
        (DataType::Duration(_), DataType::Int64) |
        // Apply "large" arrow dtypes on their "small" variants
        (DataType::LargeUtf8, DataType::Utf8) |
        (DataType::LargeBinary, DataType::Binary) |
        (DataType::Decimal256(..), DataType::Decimal(..)) => Field::new(
            inferred_field.name.clone(),
            origin_field.data_type().clone(),
            inferred_field.is_nullable,
        ),

        // If DataTypes are recursive, apply to the children
        (DataType::List(origin_subfield), DataType::List(inferred_subfield)) => Field::new(
            inferred_field.name.clone(),
            DataType::List(Box::new(apply_original_metadata(origin_subfield, inferred_subfield.as_ref()))),
            inferred_field.is_nullable,
        ),
        (DataType::LargeList(origin_subfield), DataType::List(inferred_subfield)) => Field::new(
            inferred_field.name.clone(),
            DataType::LargeList(Box::new(apply_original_metadata(origin_subfield, inferred_subfield.as_ref()))),
            inferred_field.is_nullable,
        ),
        (DataType::FixedSizeList(origin_subfield, size), DataType::List(inferred_subfield)) => Field::new(
            inferred_field.name.clone(),
            DataType::FixedSizeList(Box::new(apply_original_metadata(origin_subfield, inferred_subfield.as_ref())), *size),
            inferred_field.is_nullable,
        ),
        (DataType::Struct(origin_subfields), DataType::Struct(inferred_subfields)) => {
            Field::new(
                inferred_field.name.clone(),
                DataType::Struct(origin_subfields.iter().zip(inferred_subfields.iter()).map(|(o, i)| apply_original_metadata(o, i)).collect::<Vec<_>>()),
                inferred_field.is_nullable,
            )
        },

        // No matches indicate that the inferred field is consistent with the arrow_schema and doesn't require modifications
        _ => inferred_field.clone(),
    };

    // Apply metadata (preferentially from the inferred field in the case of key conflicts)
    let mut new_metadata = BTreeMap::new();
    for (k, v) in origin_field.metadata.iter() {
        new_metadata.insert(k.clone(), v.clone());
    }
    for (k, v) in inferred_field.metadata.iter() {
        new_metadata.insert(k.clone(), v.clone());
    }

    new_field.with_metadata(new_metadata)
}

/// Apply an Arrow schema on inferred fields
pub fn apply_schema_to_fields(schema: &Schema, fields: &[Field]) -> Vec<Field> {
    schema
        .fields
        .iter()
        .zip(fields.iter())
        .map(|(origin_field, inferred_field)| apply_original_metadata(origin_field, inferred_field))
        .collect::<Vec<_>>()
}
