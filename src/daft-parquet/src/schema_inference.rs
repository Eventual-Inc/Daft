use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Infer an arrow-rs schema from parquet metadata, with Daft-specific
/// post-processing (timestamp coercion, large-offset promotion, dictionary
/// stripping, raw string-as-binary).
pub fn infer_schema_from_parquet_metadata_arrowrs(
    metadata: &parquet::file::metadata::ParquetMetaData,
    coerce_int96_timestamp_unit: Option<daft_core::prelude::TimeUnit>,
    string_encoding_raw: bool,
) -> Result<Schema, arrow::error::ArrowError> {
    let file_metadata = metadata.file_metadata();
    let mut arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    // Coerce INT96 timestamps (decoded as Nanosecond) to the requested unit.
    if let Some(target) = coerce_int96_timestamp_unit {
        let target = target.to_arrow();
        if target != TimeUnit::Nanosecond {
            arrow_schema = walk_schema(&arrow_schema, &|dt| match dt {
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    Some(DataType::Timestamp(target, tz.clone()))
                }
                _ => None,
            });
        }
    }

    // Raw string encoding: surface BYTE_ARRAY as Binary instead of Utf8.
    if string_encoding_raw {
        arrow_schema = walk_schema(&arrow_schema, &|dt| match dt {
            DataType::Utf8 => Some(DataType::Binary),
            DataType::LargeUtf8 => Some(DataType::LargeBinary),
            _ => None,
        });
    }

    // Strip dictionary encoding (Daft has no Dictionary type). Must run before
    // large-offset promotion so Dictionary(_, Utf8) → Utf8 → LargeUtf8.
    arrow_schema = walk_schema(&arrow_schema, &|dt| match dt {
        DataType::Dictionary(_, value_type) => Some((**value_type).clone()),
        _ => None,
    });

    // Promote small-offset types to large-offset. Note: List → LargeList is
    // intentionally NOT done here — arrow-rs `with_schema()` doesn't support
    // that coercion; Daft's `ListArray::from_arrow` widens offsets at construction.
    arrow_schema = walk_schema(&arrow_schema, &|dt| match dt {
        DataType::Utf8 => Some(DataType::LargeUtf8),
        DataType::Binary => Some(DataType::LargeBinary),
        _ => None,
    });

    Ok(arrow_schema)
}

/// Apply `transform` to every datatype reachable from the schema's fields,
/// preserving non-matching types. If `transform` returns `Some(new)`, the
/// replacement is itself walked (so multi-stage transforms like
/// `Dictionary(_, Dictionary(_, V)) → V` converge).
fn walk_schema<F>(schema: &Schema, transform: &F) -> Schema
where
    F: Fn(&DataType) -> Option<DataType>,
{
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| walk_field(f, transform).unwrap_or_else(|| f.as_ref().clone()))
        .collect();
    Schema::new_with_metadata(new_fields, schema.metadata().clone())
}

fn walk_field<F>(field: &Field, transform: &F) -> Option<Field>
where
    F: Fn(&DataType) -> Option<DataType>,
{
    walk_datatype(field.data_type(), transform).map(|dt| field.as_ref().clone().with_data_type(dt))
}

fn walk_datatype<F>(dt: &DataType, transform: &F) -> Option<DataType>
where
    F: Fn(&DataType) -> Option<DataType>,
{
    // Leaf transform takes precedence; recurse into the replacement in case
    // it contains more matches (e.g. nested Dictionary).
    if let Some(replaced) = transform(dt) {
        return Some(walk_datatype(&replaced, transform).unwrap_or(replaced));
    }
    match dt {
        DataType::List(f) => walk_field(f, transform).map(|f| DataType::List(f.into())),
        DataType::LargeList(f) => walk_field(f, transform).map(|f| DataType::LargeList(f.into())),
        DataType::FixedSizeList(f, size) => {
            walk_field(f, transform).map(|f| DataType::FixedSizeList(f.into(), *size))
        }
        DataType::Map(f, sorted) => {
            walk_field(f, transform).map(|f| DataType::Map(f.into(), *sorted))
        }
        DataType::Struct(fields) => {
            let new: Vec<Field> = fields
                .iter()
                .map(|f| walk_field(f, transform).unwrap_or_else(|| f.as_ref().clone()))
                .collect();
            let changed = new.iter().zip(fields.iter()).any(|(a, b)| a != b.as_ref());
            changed.then(|| DataType::Struct(new.into()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    fn coerce_ts(target: TimeUnit) -> impl Fn(&DataType) -> Option<DataType> {
        move |dt: &DataType| match dt {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Some(DataType::Timestamp(target, tz.clone()))
            }
            _ => None,
        }
    }

    fn strings_to_binary(dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Utf8 => Some(DataType::Binary),
            DataType::LargeUtf8 => Some(DataType::LargeBinary),
            _ => None,
        }
    }

    fn strip_dict(dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Dictionary(_, v) => Some((**v).clone()),
            _ => None,
        }
    }

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
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        let result = walk_schema(&schema, &coerce_ts(TimeUnit::Microsecond));
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
        let result = walk_schema(&schema, &strings_to_binary);
        assert_eq!(result.field(0).data_type(), &DataType::Binary);
        assert_eq!(result.field(1).data_type(), &DataType::LargeBinary);
        assert_eq!(result.field(2).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_coerce_nested_timestamp() {
        let inner = Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        );
        let schema = Schema::new(vec![Field::new(
            "list_col",
            DataType::List(Arc::new(inner)),
            true,
        )]);
        let result = walk_schema(&schema, &coerce_ts(TimeUnit::Millisecond));
        match result.field(0).data_type() {
            DataType::List(inner) => assert_eq!(
                inner.data_type(),
                &DataType::Timestamp(TimeUnit::Millisecond, None)
            ),
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
        let result = walk_schema(&schema, &strings_to_binary);
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
        let result = walk_schema(&schema, &strip_dict);
        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
        assert_eq!(result.field(1).data_type(), &DataType::Int32);
        match result.field(2).data_type() {
            DataType::List(inner) => assert_eq!(inner.data_type(), &DataType::Utf8),
            other => panic!("Expected List, got {:?}", other),
        }
    }

    #[test]
    fn test_nested_dictionary_converges() {
        // Dictionary(_, Dictionary(_, Utf8)) should fully strip.
        let nested = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let schema = Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(nested)),
            true,
        )]);
        let result = walk_schema(&schema, &strip_dict);
        assert_eq!(result.field(0).data_type(), &DataType::Utf8);
    }
}
