use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

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
            arrow_schema = transform_schema(&arrow_schema, &|dt| match dt {
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    Some(DataType::Timestamp(target_arrow_unit, tz.clone()))
                }
                _ => None,
            });
        }
    }

    // Post-process: convert string types to binary if raw encoding is requested
    if string_encoding_raw {
        arrow_schema = transform_schema(&arrow_schema, &|dt| match dt {
            DataType::Utf8 => Some(DataType::Binary),
            DataType::LargeUtf8 => Some(DataType::LargeBinary),
            _ => None,
        });
    }

    // Post-process: strip dictionary encoding. Arrow-rs parquet reader can infer
    // Dictionary(Int32, Utf8) etc. for dictionary-encoded columns. Daft doesn't
    // support dictionary types, so unwrap to the value type.
    // This must run before large-offset promotion so Dictionary(_, Utf8) → Utf8 → LargeUtf8.
    // Uses its own walker (not `transform_schema`) because unwrapping a Dictionary can
    // expose nested containers whose children may themselves be Dictionary — e.g.
    // `Dictionary<List<Dictionary<Utf8>>>`. The generic helper short-circuits after the
    // outer rewrite and would leave the inner Dictionary intact.
    arrow_schema = strip_dictionaries_schema(&arrow_schema);

    // Post-process: promote small-offset types to large-offset types.
    // Arrow-rs parquet reader produces Utf8/Binary/List (i32 offsets) by default,
    // but Daft expects LargeUtf8/LargeBinary/LargeList (i64 offsets).
    // By putting the large types in the schema we pass to ArrowReaderOptions::with_schema(),
    // arrow-rs will produce the large types directly, avoiding a post-decode cast.
    //
    // arrow-rs `with_schema()` can promote Utf8→LargeUtf8 and Binary→LargeBinary,
    // but does NOT support List→LargeList coercion — so we keep List containers as-is
    // and only promote their inner element types. Daft's ListArray::from_arrow handles
    // i32→i64 offset widening transparently.
    arrow_schema = transform_schema(&arrow_schema, &|dt| match dt {
        DataType::Utf8 => Some(DataType::LargeUtf8),
        DataType::Binary => Some(DataType::LargeBinary),
        _ => None,
    });

    Ok(arrow_schema)
}

/// Recursively strip `Dictionary(_, value)` anywhere in the schema, descending through
/// containers so nested dicts inside the unwrapped value get stripped too.
fn strip_dictionaries_schema(schema: &Schema) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| strip_dictionaries_field(f.as_ref()))
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn strip_dictionaries_field(field: &Field) -> Field {
    field
        .clone()
        .with_data_type(strip_dictionaries_datatype(field.data_type()))
}

fn strip_dictionaries_datatype(dtype: &DataType) -> DataType {
    match dtype {
        DataType::Dictionary(_, v) => strip_dictionaries_datatype(v),
        DataType::List(inner) => DataType::List(strip_dictionaries_field(inner.as_ref()).into()),
        DataType::LargeList(inner) => {
            DataType::LargeList(strip_dictionaries_field(inner.as_ref()).into())
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(strip_dictionaries_field(inner.as_ref()).into(), *size)
        }
        DataType::Map(inner, sorted) => {
            DataType::Map(strip_dictionaries_field(inner.as_ref()).into(), *sorted)
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| strip_dictionaries_field(f.as_ref()))
                .collect();
            DataType::Struct(new_fields.into())
        }
        other => other.clone(),
    }
}

/// Apply `transform` to every datatype in the schema. If `transform` returns `Some`,
/// the result replaces the input type as-is (no further recursion into its children —
/// the caller is responsible for producing a fully-transformed type). If it returns
/// `None`, we recurse into nested children (List/LargeList/FixedSizeList/Struct/Map).
fn transform_schema(schema: &Schema, transform: &impl Fn(&DataType) -> Option<DataType>) -> Schema {
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| transform_field(f.as_ref(), transform))
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn transform_field(field: &Field, transform: &impl Fn(&DataType) -> Option<DataType>) -> Field {
    match transform_datatype(field.data_type(), transform) {
        Some(dt) => field.clone().with_data_type(dt),
        None => field.clone(),
    }
}

fn transform_datatype(
    dtype: &DataType,
    transform: &impl Fn(&DataType) -> Option<DataType>,
) -> Option<DataType> {
    if let Some(new_dt) = transform(dtype) {
        return Some(new_dt);
    }
    match dtype {
        DataType::List(inner) => {
            let new_inner = transform_field(inner.as_ref(), transform);
            (&new_inner != inner.as_ref()).then(|| DataType::List(new_inner.into()))
        }
        DataType::LargeList(inner) => {
            let new_inner = transform_field(inner.as_ref(), transform);
            (&new_inner != inner.as_ref()).then(|| DataType::LargeList(new_inner.into()))
        }
        DataType::FixedSizeList(inner, size) => {
            let new_inner = transform_field(inner.as_ref(), transform);
            (&new_inner != inner.as_ref()).then(|| DataType::FixedSizeList(new_inner.into(), *size))
        }
        DataType::Map(inner, sorted) => {
            let new_inner = transform_field(inner.as_ref(), transform);
            (&new_inner != inner.as_ref()).then(|| DataType::Map(new_inner.into(), *sorted))
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| transform_field(f.as_ref(), transform))
                .collect();
            new_fields
                .iter()
                .zip(fields.iter())
                .any(|(a, b)| a != b.as_ref())
                .then(|| DataType::Struct(new_fields.into()))
        }
        _ => None,
    }
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
        let result = transform_schema(&schema, &|dt| match dt {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Some(DataType::Timestamp(target, tz.clone()))
            }
            _ => None,
        });

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

    fn strings_to_binary_leaf(dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Utf8 => Some(DataType::Binary),
            DataType::LargeUtf8 => Some(DataType::LargeBinary),
            _ => None,
        }
    }

    #[test]
    fn test_coerce_strings_to_binary() {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("i", DataType::Int32, true),
        ]);
        let result = transform_schema(&schema, &strings_to_binary_leaf);

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
        let result = transform_schema(&schema, &|dt| match dt {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Some(DataType::Timestamp(target, tz.clone()))
            }
            _ => None,
        });

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
        let result = transform_schema(&schema, &strings_to_binary_leaf);

        match result.field(0).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields[0].data_type(), &DataType::Binary);
                assert_eq!(fields[1].data_type(), &DataType::Int64);
            }
            other => panic!("Expected Struct, got {:?}", other),
        }
    }

    #[test]
    fn test_strip_dictionary_types() {
        let schema = Schema::new(vec![
            Field::new(
                "dict_str",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("plain_int", DataType::Int32, true),
            Field::new(
                "list_of_dict",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ))),
                true,
            ),
        ]);
        let result = strip_dictionaries_schema(&schema);

        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
        assert_eq!(result.field(1).data_type(), &DataType::Int32);
        match result.field(2).data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_strip_dictionary_nested_in_value_type() {
        // Dictionary<Int32, List<Dictionary<Int8, Utf8>>> — the inner Dictionary lives
        // inside the outer Dictionary's value type. Stripping must descend through the
        // unwrapped List to remove it.
        let inner_dict = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let outer = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::new(Field::new(
                "item", inner_dict, true,
            )))),
        );
        let schema = Schema::new(vec![Field::new("col", outer, true)]);

        let result = strip_dictionaries_schema(&schema);

        match result.field(0).data_type() {
            DataType::List(inner) => assert_eq!(inner.data_type(), &DataType::Utf8),
            other => panic!("Expected List, got {:?}", other),
        }
    }
}
