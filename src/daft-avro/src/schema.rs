use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use daft_core::datatypes::{DataType, Field as DaftField};
use daft_schema::schema::{Schema as DaftSchema, SchemaRef};

use crate::{AvroError, Result};

/// Convert an Avro record schema to a Daft schema.
pub fn avro_schema_to_daft_schema(avro_schema: &AvroSchema) -> Result<DaftSchema> {
    let fields = match avro_schema {
        AvroSchema::Record(record_schema) => record_schema
            .fields
            .iter()
            .map(|f| avro_field_to_daft_field(&f.name, &f.schema))
            .collect::<Result<Vec<_>>>()?,
        other => {
            return Err(AvroError::SchemaConversionError {
                message: format!(
                    "Expected a record schema for file-level conversion, got: {:?}",
                    other
                ),
            });
        }
    };
    Ok(DaftSchema::new(fields))
}

/// Convert an Avro schema element to a Daft field.
pub fn avro_field_to_daft_field(name: &str, schema: &AvroSchema) -> Result<DaftField> {
    let dtype = avro_type_to_daft_type(schema)?;
    Ok(DaftField::new(name.to_string(), dtype))
}

/// Convert an Avro schema type to a Daft DataType.
pub fn avro_type_to_daft_type(schema: &AvroSchema) -> Result<DataType> {
    match schema {
        AvroSchema::Null => Ok(DataType::Null),
        AvroSchema::Boolean => Ok(DataType::Boolean),
        AvroSchema::Int => Ok(DataType::Int32),
        AvroSchema::Long => Ok(DataType::Int64),
        AvroSchema::Float => Ok(DataType::Float32),
        AvroSchema::Double => Ok(DataType::Float64),
        AvroSchema::Bytes => Ok(DataType::Binary),
        AvroSchema::String => Ok(DataType::Utf8),
        AvroSchema::Array(array_schema) => {
            let item_type = avro_type_to_daft_type(&array_schema.items)?;
            Ok(DataType::List(Box::new(item_type)))
        }
        AvroSchema::Map(map_schema) => {
            let value_type = avro_type_to_daft_type(&map_schema.types)?;
            Ok(DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(value_type),
            })
        }
        AvroSchema::Record(record_schema) => {
            let daft_fields: Vec<DaftField> = record_schema
                .fields
                .iter()
                .map(|f| avro_field_to_daft_field(&f.name, &f.schema))
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(daft_fields))
        }
        AvroSchema::Enum { .. } => Ok(DataType::Utf8),
        AvroSchema::Fixed(fixed_schema) => Ok(DataType::FixedSizeBinary(fixed_schema.size)),
        AvroSchema::Union(union_schema) => {
            let non_null: Vec<&AvroSchema> = union_schema
                .variants()
                .iter()
                .filter(|t| !matches!(t, AvroSchema::Null))
                .collect();
            match non_null.len() {
                0 => Ok(DataType::Null),
                1 => avro_type_to_daft_type(non_null[0]),
                _ => {
                    // Multi-branch union: fall back to struct with nullable fields
                    let fields: Vec<DaftField> = non_null
                        .iter()
                        .enumerate()
                        .map(|(i, s)| avro_field_to_daft_field(&format!("member{}", i), s))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(DataType::Struct(fields))
                }
            }
        }
        AvroSchema::Ref { name } => Err(AvroError::SchemaConversionError {
            message: format!("Unresolved schema reference: {}", name.fullname(None)),
        }),
        _ => Err(AvroError::UnsupportedType {
            type_name: format!("{:?}", schema),
        }),
    }
}

/// Convert a Daft schema to an Avro record schema.
pub fn daft_schema_to_avro_schema(
    daft_schema: &DaftSchema,
    record_name: &str,
    record_namespace: &str,
) -> Result<AvroSchema> {
    let fields: Vec<apache_avro::schema::RecordField> = daft_schema
        .fields()
        .iter()
        .map(daft_field_to_avro_field)
        .collect::<Result<Vec<_>>>()?;

    let lookup: std::collections::BTreeMap<String, usize> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name.clone(), i))
        .collect();

    Ok(AvroSchema::Record(apache_avro::schema::RecordSchema {
        name: apache_avro::schema::Name {
            name: record_name.to_string(),
            namespace: if record_namespace.is_empty() {
                None
            } else {
                Some(record_namespace.to_string())
            },
        },
        aliases: None,
        doc: None,
        fields,
        lookup,
        attributes: Default::default(),
    }))
}

fn daft_field_to_avro_field(field: &DaftField) -> Result<apache_avro::schema::RecordField> {
    let inner_schema = daft_type_to_avro_type(&field.dtype, Some(&field.name))?;
    // Make the field nullable by wrapping it in a union [Null, Type].
    // If the type is already Null, use String as the non-null variant since
    // Avro doesn't support pure Null record fields.
    let inner_schema = if inner_schema == AvroSchema::Null {
        AvroSchema::String
    } else {
        inner_schema
    };
    let schema = AvroSchema::Union(
        apache_avro::schema::UnionSchema::new(vec![AvroSchema::Null, inner_schema]).map_err(
            |e| AvroError::SchemaConversionError {
                message: format!("Failed to create union schema: {}", e),
            },
        )?,
    );
    Ok(apache_avro::schema::RecordField {
        name: field.name.to_string(),
        doc: None,
        aliases: None,
        default: None,
        schema,
        order: apache_avro::schema::RecordFieldOrder::Ignore,
        // Position is set to 0 here and populated by the caller via the
        // enclosing RecordSchema's lookup map. The actual field ordering
        // is determined by the `fields` Vec iteration order in the schema.
        position: 0,
        custom_attributes: Default::default(),
    })
}

fn daft_type_to_avro_type(dtype: &DataType, record_name_hint: Option<&str>) -> Result<AvroSchema> {
    match dtype {
        DataType::Null => Ok(AvroSchema::Null),
        DataType::Boolean => Ok(AvroSchema::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Ok(AvroSchema::Int),
        DataType::UInt8 | DataType::UInt16 => Ok(AvroSchema::Int),
        DataType::Int64 => Ok(AvroSchema::Long),
        DataType::UInt32 | DataType::UInt64 => Ok(AvroSchema::Long),
        DataType::Float32 => Ok(AvroSchema::Float),
        DataType::Float64 => Ok(AvroSchema::Double),
        DataType::Utf8 => Ok(AvroSchema::String),
        DataType::Binary => Ok(AvroSchema::Bytes),
        DataType::FixedSizeBinary(size) => {
            Ok(AvroSchema::Fixed(apache_avro::schema::FixedSchema {
                name: apache_avro::schema::Name::new("fixed").map_err(|e| {
                    AvroError::SchemaConversionError {
                        message: format!("Invalid fixed name: {}", e),
                    }
                })?,
                aliases: None,
                doc: None,
                size: *size,
                default: None,
                attributes: Default::default(),
            }))
        }
        DataType::Date => Ok(AvroSchema::Int),
        DataType::Struct(fields) => {
            let avro_fields: Vec<apache_avro::schema::RecordField> = fields
                .iter()
                .map(daft_field_to_avro_field)
                .collect::<Result<Vec<_>>>()?;
            let lookup: std::collections::BTreeMap<String, usize> = avro_fields
                .iter()
                .enumerate()
                .map(|(i, f)| (f.name.clone(), i))
                .collect();
            let record_name = record_name_hint
                .map(|n| format!("{}_record", n))
                .unwrap_or_else(|| "nested_record".to_string());
            Ok(AvroSchema::Record(apache_avro::schema::RecordSchema {
                name: apache_avro::schema::Name {
                    name: record_name,
                    namespace: None,
                },
                aliases: None,
                doc: None,
                fields: avro_fields,
                lookup,
                attributes: Default::default(),
            }))
        }
        DataType::List(child) => {
            let item_schema = daft_type_to_avro_type(child, None)?;
            Ok(AvroSchema::Array(apache_avro::schema::ArraySchema {
                items: Box::new(item_schema),
                attributes: Default::default(),
            }))
        }
        DataType::Map { key: _, value } => {
            let value_schema = daft_type_to_avro_type(value, None)?;
            Ok(AvroSchema::Map(apache_avro::schema::MapSchema {
                types: Box::new(value_schema),
                attributes: Default::default(),
            }))
        }
        _ => Err(AvroError::SchemaConversionError {
            message: format!("Unsupported Daft type for Avro conversion: {:?}", dtype),
        }),
    }
}

/// Read the Avro schema from an OCF file header without reading data.
pub async fn read_avro_schema(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
) -> Result<SchemaRef> {
    let get_result = io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await
        .map_err(|e| AvroError::IOError { source: e })?;

    use daft_io::GetResult;
    let bytes = match get_result {
        GetResult::File(file) => {
            tokio::fs::read(file.path)
                .await
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!("Failed to read Avro schema file: {}", e),
                })?
        }
        GetResult::Stream(stream, ..) => {
            use futures::StreamExt;
            let mut data = Vec::new();
            futures::pin_mut!(stream);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| AvroError::IOError { source: e })?;
                data.extend_from_slice(&chunk);
            }
            data
        }
    };

    let reader = apache_avro::Reader::new(&bytes[..]).map_err(|e| AvroError::ReadError {
        source: Box::new(e),
    })?;

    let avro_schema = reader.writer_schema();
    let daft_schema = avro_schema_to_daft_schema(avro_schema)?;
    Ok(Arc::new(daft_schema))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use apache_avro::{
        Schema as AvroSchema,
        schema::{Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema},
    };
    use daft_core::datatypes::DataType;
    use daft_schema::field::Field;

    use super::*;

    fn make_record_field(name: &str, schema: AvroSchema) -> RecordField {
        RecordField {
            name: name.to_string(),
            doc: None,
            aliases: None,
            default: None,
            schema,
            order: RecordFieldOrder::Ignore,
            position: 0,
            custom_attributes: BTreeMap::new(),
        }
    }

    fn make_record_schema(name: &str, fields: Vec<RecordField>) -> AvroSchema {
        let lookup: BTreeMap<String, usize> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        AvroSchema::Record(RecordSchema {
            name: Name::new(name).unwrap(),
            aliases: None,
            doc: None,
            fields,
            lookup,
            attributes: BTreeMap::new(),
        })
    }

    // === Basic type mapping: Avro → Daft ===

    #[test]
    fn test_basic_types_null() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Null)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Null);
    }

    #[test]
    fn test_basic_types_boolean() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Boolean)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Boolean);
    }

    #[test]
    fn test_basic_types_int() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Int)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Int32);
    }

    #[test]
    fn test_basic_types_long() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Long)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Int64);
    }

    #[test]
    fn test_basic_types_float() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Float)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Float32);
    }

    #[test]
    fn test_basic_types_double() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Double)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Float64);
    }

    #[test]
    fn test_basic_types_string() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::String)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Utf8);
    }

    #[test]
    fn test_basic_types_bytes() {
        let schema = make_record_schema("r", vec![make_record_field("col", AvroSchema::Bytes)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Binary);
    }

    // === Complex types ===

    #[test]
    fn test_complex_type_array() {
        let array_schema = AvroSchema::Array(apache_avro::schema::ArraySchema {
            items: Box::new(AvroSchema::String),
            attributes: BTreeMap::new(),
        });
        let schema = make_record_schema("r", vec![make_record_field("col", array_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(
            daft.fields()[0].dtype,
            DataType::List(Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn test_complex_type_map() {
        let map_schema = AvroSchema::Map(apache_avro::schema::MapSchema {
            types: Box::new(AvroSchema::Long),
            attributes: BTreeMap::new(),
        });
        let schema = make_record_schema("r", vec![make_record_field("col", map_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(
            daft.fields()[0].dtype,
            DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Int64),
            }
        );
    }

    #[test]
    fn test_complex_type_record() {
        let nested = make_record_schema("nested", vec![make_record_field("x", AvroSchema::Int)]);
        let schema = make_record_schema("r", vec![make_record_field("col", nested)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        let expected = DataType::Struct(vec![Field::new("x", DataType::Int32)]);
        assert_eq!(daft.fields()[0].dtype, expected);
    }

    #[test]
    fn test_complex_type_enum() {
        let enum_schema = AvroSchema::Enum(apache_avro::schema::EnumSchema {
            name: Name::new("E").unwrap(),
            aliases: None,
            doc: None,
            symbols: vec!["A".to_string(), "B".to_string()],
            default: None,
            attributes: BTreeMap::new(),
        });
        let schema = make_record_schema("r", vec![make_record_field("col", enum_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Utf8);
    }

    // === Union handling ===

    #[test]
    fn test_union_nullable_single_type() {
        // Union [Null, String] → String
        let union_schema = AvroSchema::Union(
            UnionSchema::new(vec![AvroSchema::Null, AvroSchema::String]).unwrap(),
        );
        let schema = make_record_schema("r", vec![make_record_field("col", union_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Utf8);
    }

    #[test]
    fn test_union_all_null() {
        // Union [Null] → Null
        let union_schema = AvroSchema::Union(UnionSchema::new(vec![AvroSchema::Null]).unwrap());
        let schema = make_record_schema("r", vec![make_record_field("col", union_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        assert_eq!(daft.fields()[0].dtype, DataType::Null);
    }

    #[test]
    fn test_union_multi_branch() {
        // Multi-branch union → Struct with member0, member1, ...
        let union_schema = AvroSchema::Union(
            UnionSchema::new(vec![AvroSchema::Null, AvroSchema::String, AvroSchema::Int]).unwrap(),
        );
        let schema = make_record_schema("r", vec![make_record_field("col", union_schema)]);
        let daft = avro_schema_to_daft_schema(&schema).unwrap();
        let expected = DataType::Struct(vec![
            Field::new("member0", DataType::Utf8),
            Field::new("member1", DataType::Int32),
        ]);
        assert_eq!(daft.fields()[0].dtype, expected);
    }

    // === Daft → Avro bidirectional ===

    #[test]
    fn test_daft_to_avro_bidirectional_basic() {
        // Round-trip: Daft → Avro → Daft for basic types
        let daft_fields = vec![
            Field::new("a", DataType::Boolean),
            Field::new("b", DataType::Int32),
            Field::new("c", DataType::Int64),
            Field::new("d", DataType::Float32),
            Field::new("e", DataType::Float64),
            Field::new("f", DataType::Utf8),
            Field::new("g", DataType::Binary),
        ];
        let daft_schema = daft_schema::schema::Schema::new(daft_fields);

        let avro = daft_schema_to_avro_schema(&daft_schema, "record", "").unwrap();
        let round_tripped = avro_schema_to_daft_schema(&avro).unwrap();

        for original in daft_schema.fields() {
            let rt = round_tripped
                .fields()
                .iter()
                .find(|f| f.name == original.name)
                .unwrap();
            assert_eq!(
                rt.dtype, original.dtype,
                "mismatch for field {}",
                original.name
            );
        }
    }

    #[test]
    fn test_daft_to_avro_bidirectional_nested_struct() {
        let daft_fields = vec![Field::new(
            "inner",
            DataType::Struct(vec![
                Field::new("x", DataType::Int32),
                Field::new("y", DataType::Utf8),
            ]),
        )];
        let daft_schema = daft_schema::schema::Schema::new(daft_fields);

        let avro = daft_schema_to_avro_schema(&daft_schema, "record", "").unwrap();
        let round_tripped = avro_schema_to_daft_schema(&avro).unwrap();

        let inner = &round_tripped.fields()[0];
        match &inner.dtype {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name.as_ref(), "x");
                assert_eq!(fields[0].dtype, DataType::Int32);
                assert_eq!(fields[1].name.as_ref(), "y");
                assert_eq!(fields[1].dtype, DataType::Utf8);
            }
            other => panic!("expected Struct, got {:?}", other),
        }
    }

    #[test]
    fn test_daft_to_avro_with_namespace() {
        let daft_fields = vec![Field::new("col", DataType::Int32)];
        let daft_schema = daft_schema::schema::Schema::new(daft_fields);

        let avro = daft_schema_to_avro_schema(&daft_schema, "my_record", "com.example").unwrap();

        if let AvroSchema::Record(rs) = &avro {
            assert_eq!(rs.name.name, "my_record");
            assert_eq!(rs.name.namespace, Some("com.example".to_string()));
        } else {
            panic!("expected Record schema");
        }
    }

    #[test]
    fn test_daft_to_avro_empty_namespace() {
        let daft_fields = vec![Field::new("col", DataType::Int32)];
        let daft_schema = daft_schema::schema::Schema::new(daft_fields);

        let avro = daft_schema_to_avro_schema(&daft_schema, "my_record", "").unwrap();

        if let AvroSchema::Record(rs) = &avro {
            assert_eq!(rs.name.name, "my_record");
            assert_eq!(rs.name.namespace, None);
        } else {
            panic!("expected Record schema");
        }
    }

    #[test]
    fn test_nested_struct_unique_names() {
        // Two sibling struct fields should NOT both be named "struct"
        let daft_fields = vec![
            Field::new(
                "addr",
                DataType::Struct(vec![Field::new("street", DataType::Utf8)]),
            ),
            Field::new(
                "contact",
                DataType::Struct(vec![Field::new("phone", DataType::Utf8)]),
            ),
        ];
        let daft_schema = daft_schema::schema::Schema::new(daft_fields);

        let avro = daft_schema_to_avro_schema(&daft_schema, "root", "").unwrap();

        // Collect all record names in the Avro schema tree
        fn collect_record_names(schema: &AvroSchema, names: &mut Vec<String>) {
            if let AvroSchema::Record(rs) = schema {
                names.push(rs.name.name.clone());
                for f in &rs.fields {
                    collect_record_names(&f.schema, names);
                }
            } else if let AvroSchema::Union(u) = schema {
                for v in u.variants() {
                    collect_record_names(v, names);
                }
            } else if let AvroSchema::Array(a) = schema {
                collect_record_names(&a.items, names);
            } else if let AvroSchema::Map(m) = schema {
                collect_record_names(&m.types, names);
            }
        }

        let mut names = Vec::new();
        collect_record_names(&avro, &mut names);

        // Should contain "addr_record" and "contact_record", NOT duplicate "struct"
        assert!(names.contains(&"addr_record".to_string()));
        assert!(names.contains(&"contact_record".to_string()));
        // No name collision
        let unique: std::collections::HashSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "record names must be unique: {:?}",
            names
        );
    }
}
