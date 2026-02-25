use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common_error::DaftResult;

/// Infer an arrow-rs schema from arrow-rs parquet metadata, with optional post-processing
/// for INT96 timestamp coercion and raw string encoding.
///
/// This is the arrow-rs native alternative to `infer_arrow_schema_from_metadata` which uses
/// arrow2-based inference.
pub fn infer_schema_from_parquet_metadata_arrowrs(
    metadata: &parquet::file::metadata::ParquetMetaData,
    coerce_int96_timestamp_unit: Option<daft_core::prelude::TimeUnit>,
    string_encoding_raw: bool,
) -> Result<Schema, arrow::error::ArrowError> {
    let file_metadata = metadata.file_metadata();
    let schema_descr = file_metadata.schema_descr();
    let key_value_metadata = file_metadata.key_value_metadata();

    // Use the arrow-rs parquet crate's built-in conversion
    let mut arrow_schema =
        parquet::arrow::parquet_to_arrow_schema(schema_descr, key_value_metadata)?;

    // Post-process: coerce INT96 timestamps (which come as Nanosecond) to the desired unit
    if let Some(target_unit) = coerce_int96_timestamp_unit {
        let target_arrow_unit = target_unit.to_arrow();
        if target_arrow_unit != TimeUnit::Nanosecond {
            let new_fields: Vec<Field> = arrow_schema
                .fields()
                .iter()
                .map(|f| coerce_timestamp_field(f.as_ref(), &target_arrow_unit))
                .collect();
            arrow_schema = Schema::new_with_metadata(new_fields, arrow_schema.metadata().clone());
        }
    }

    // Post-process: convert string types to binary if raw encoding is requested
    if string_encoding_raw {
        let new_fields: Vec<Field> = arrow_schema
            .fields()
            .iter()
            .map(|f| coerce_strings_to_binary_field(f.as_ref()))
            .collect();
        arrow_schema = Schema::new_with_metadata(new_fields, arrow_schema.metadata().clone());
    }

    // Post-process: promote small-offset types to large-offset types.
    // Arrow-rs parquet reader produces Utf8/Binary/List (i32 offsets) by default,
    // but Daft expects LargeUtf8/LargeBinary/LargeList (i64 offsets).
    // By putting the large types in the schema we pass to ArrowReaderOptions::with_schema(),
    // arrow-rs will produce the large types directly, avoiding a post-decode cast.
    {
        let new_fields: Vec<Field> = arrow_schema
            .fields()
            .iter()
            .map(|f| promote_to_large_offsets_field(f.as_ref()))
            .collect();
        arrow_schema = Schema::new_with_metadata(new_fields, arrow_schema.metadata().clone());
    }

    Ok(arrow_schema)
}

/// Recursively coerce all `Timestamp(Nanosecond, tz)` fields to `Timestamp(target_unit, tz)`.
fn coerce_timestamp_field(field: &Field, target_unit: &TimeUnit) -> Field {
    let new_dtype = coerce_timestamp_datatype(field.data_type(), target_unit);
    match new_dtype {
        Some(dt) => field.as_ref().clone().with_data_type(dt),
        None => field.as_ref().clone(),
    }
}

fn coerce_timestamp_datatype(dtype: &DataType, target_unit: &TimeUnit) -> Option<DataType> {
    match dtype {
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            Some(DataType::Timestamp(*target_unit, tz.clone()))
        }
        DataType::List(inner) => {
            let new_inner = coerce_timestamp_field(inner.as_ref(), target_unit);
            if &new_inner != inner.as_ref() {
                Some(DataType::List(new_inner.into()))
            } else {
                None
            }
        }
        DataType::LargeList(inner) => {
            let new_inner = coerce_timestamp_field(inner.as_ref(), target_unit);
            if &new_inner != inner.as_ref() {
                Some(DataType::LargeList(new_inner.into()))
            } else {
                None
            }
        }
        DataType::FixedSizeList(inner, size) => {
            let new_inner = coerce_timestamp_field(inner.as_ref(), target_unit);
            if &new_inner != inner.as_ref() {
                Some(DataType::FixedSizeList(new_inner.into(), *size))
            } else {
                None
            }
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| coerce_timestamp_field(f.as_ref(), target_unit))
                .collect();
            if new_fields
                .iter()
                .zip(fields.iter())
                .any(|(a, b)| a != b.as_ref())
            {
                Some(DataType::Struct(new_fields.into()))
            } else {
                None
            }
        }
        DataType::Map(inner, sorted) => {
            let new_inner = coerce_timestamp_field(inner.as_ref(), target_unit);
            if &new_inner != inner.as_ref() {
                Some(DataType::Map(new_inner.into(), *sorted))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Recursively promote small-offset types (Utf8, Binary, List) to large-offset
/// types (LargeUtf8, LargeBinary, LargeList).
fn promote_to_large_offsets_field(field: &Field) -> Field {
    let new_dtype = promote_to_large_offsets_datatype(field.data_type());
    match new_dtype {
        Some(dt) => field.as_ref().clone().with_data_type(dt),
        None => field.as_ref().clone(),
    }
}

fn promote_to_large_offsets_datatype(dtype: &DataType) -> Option<DataType> {
    match dtype {
        DataType::Utf8 => Some(DataType::LargeUtf8),
        DataType::Binary => Some(DataType::LargeBinary),
        DataType::List(inner) => {
            let new_inner = promote_to_large_offsets_field(inner.as_ref());
            Some(DataType::LargeList(new_inner.into()))
        }
        DataType::LargeList(inner) => {
            let new_inner = promote_to_large_offsets_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::LargeList(new_inner.into()))
            } else {
                None
            }
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| promote_to_large_offsets_field(f.as_ref()))
                .collect();
            if new_fields
                .iter()
                .zip(fields.iter())
                .any(|(a, b)| a != b.as_ref())
            {
                Some(DataType::Struct(new_fields.into()))
            } else {
                None
            }
        }
        DataType::Map(inner, sorted) => {
            let new_inner = promote_to_large_offsets_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::Map(new_inner.into(), *sorted))
            } else {
                None
            }
        }
        DataType::FixedSizeList(inner, size) => {
            let new_inner = promote_to_large_offsets_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::FixedSizeList(new_inner.into(), *size))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Recursively convert all Utf8/LargeUtf8 fields to Binary/LargeBinary.
fn coerce_strings_to_binary_field(field: &Field) -> Field {
    let new_dtype = coerce_strings_to_binary_datatype(field.data_type());
    match new_dtype {
        Some(dt) => field.as_ref().clone().with_data_type(dt),
        None => field.as_ref().clone(),
    }
}

fn coerce_strings_to_binary_datatype(dtype: &DataType) -> Option<DataType> {
    match dtype {
        DataType::Utf8 => Some(DataType::Binary),
        DataType::LargeUtf8 => Some(DataType::LargeBinary),
        DataType::List(inner) => {
            let new_inner = coerce_strings_to_binary_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::List(new_inner.into()))
            } else {
                None
            }
        }
        DataType::LargeList(inner) => {
            let new_inner = coerce_strings_to_binary_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::LargeList(new_inner.into()))
            } else {
                None
            }
        }
        DataType::FixedSizeList(inner, size) => {
            let new_inner = coerce_strings_to_binary_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::FixedSizeList(new_inner.into(), *size))
            } else {
                None
            }
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| coerce_strings_to_binary_field(f.as_ref()))
                .collect();
            if new_fields
                .iter()
                .zip(fields.iter())
                .any(|(a, b)| a != b.as_ref())
            {
                Some(DataType::Struct(new_fields.into()))
            } else {
                None
            }
        }
        DataType::Map(inner, sorted) => {
            let new_inner = coerce_strings_to_binary_field(inner.as_ref());
            if &new_inner != inner.as_ref() {
                Some(DataType::Map(new_inner.into(), *sorted))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Convert an arrow-rs `Schema` to a Daft `Schema`.
///
/// This uses the existing `TryFrom<&arrow_schema::Schema>` implementation on `daft_core::prelude::Schema`.
pub fn arrow_schema_to_daft_schema(
    arrow_schema: &Schema,
) -> DaftResult<daft_core::prelude::Schema> {
    daft_core::prelude::Schema::try_from(arrow_schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn test_coerce_timestamp_nanosecond_to_microsecond() {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "ts_tz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            // This one should NOT be coerced (already Microsecond)
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);

        let target = TimeUnit::Microsecond;
        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| coerce_timestamp_field(f.as_ref(), &target))
            .collect();
        let result = Schema::new(new_fields);

        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(result.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            result.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            result.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_strings_to_binary() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("i", DataType::Int32, true),
        ]);

        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| coerce_strings_to_binary_field(f.as_ref()))
            .collect();
        let result = Schema::new(new_fields);

        assert_eq!(result.field(0).data_type(), &DataType::Binary);
        assert_eq!(result.field(1).data_type(), &DataType::LargeBinary);
        assert_eq!(result.field(2).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_coerce_nested_timestamp() {
        let inner_field = Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        );
        let schema = Schema::new(vec![Field::new(
            "list_col",
            DataType::List(Arc::new(inner_field)),
            true,
        )]);

        let target = TimeUnit::Millisecond;
        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| coerce_timestamp_field(f.as_ref(), &target))
            .collect();
        let result = Schema::new(new_fields);

        match result.field(0).data_type() {
            DataType::List(inner) => {
                assert_eq!(
                    inner.data_type(),
                    &DataType::Timestamp(TimeUnit::Millisecond, None)
                );
            }
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_coerce_nested_strings_to_binary() {
        let struct_fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ];
        let schema = Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(struct_fields.into()),
            true,
        )]);

        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| coerce_strings_to_binary_field(f.as_ref()))
            .collect();
        let result = Schema::new(new_fields);

        match result.field(0).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields[0].data_type(), &DataType::Binary);
                assert_eq!(fields[1].data_type(), &DataType::Int64);
            }
            other => panic!("Expected Struct, got {:?}", other),
        }
    }

    #[test]
    fn test_arrow_schema_to_daft_schema() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        let daft_schema = arrow_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft_schema.len(), 2);
        assert_eq!(daft_schema.fields()[0].name, "a");
        assert_eq!(daft_schema.fields()[1].name, "b");
    }
}
