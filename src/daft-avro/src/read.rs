use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use apache_avro::{Reader, types::Value as AvroValue};
use arrow_array::{
    ArrayRef,
    builder::{
        BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
        StringBuilder,
    },
};
use daft_core::datatypes::DataType as DaftDataType;
use daft_recordbatch::RecordBatch;

use crate::{AvroError, Result, schema::avro_schema_to_daft_schema};

/// Read an Avro file into a RecordBatch.
pub async fn read_avro(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
    column_projection: Option<Vec<String>>,
    max_records: Option<usize>,
) -> Result<RecordBatch> {
    let get_result = io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await
        .map_err(|e| AvroError::IOError { source: e })?;

    let bytes = match get_result {
        daft_io::GetResult::File(file) => {
            tokio::fs::read(file.path)
                .await
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!("Failed to read Avro file: {}", e),
                })?
        }
        daft_io::GetResult::Stream(stream, ..) => {
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

    let reader = Reader::new(&bytes[..]).map_err(|e| AvroError::ReadError {
        source: Box::new(e),
    })?;

    let daft_schema = avro_schema_to_daft_schema(reader.writer_schema())?;

    // Collect rows
    let mut records: Vec<AvroValue> = Vec::new();
    for value in reader {
        if max_records.is_some_and(|max| records.len() >= max) {
            break;
        }
        let value = value.map_err(|e| AvroError::ReadError {
            source: Box::new(e),
        })?;
        records.push(value);
    }

    if records.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(daft_schema))));
    }

    // Convert Avro record values to Arrow arrays
    let arrays = avro_records_to_arrow(&records, &daft_schema, column_projection.as_ref())?;

    // Build a schema that matches the projected arrays
    let projected_schema = if let Some(ref proj) = column_projection {
        let proj_set: HashSet<&str> = proj.iter().map(|s| s.as_str()).collect();
        let fields: Vec<_> = daft_schema
            .fields()
            .iter()
            .filter(|f| proj_set.contains(f.name.as_ref()))
            .cloned()
            .collect();
        Arc::new(daft_schema::schema::Schema::new(fields))
    } else {
        Arc::new(daft_schema)
    };

    Ok(RecordBatch::from_arrow(projected_schema, arrays)?)
}

/// Normalize an Avro value by unwrapping Union types.
/// Union(Null) returns None, Union(T) returns Some(T), otherwise Some(value).
fn normalize_avro_value(value: &AvroValue) -> Option<&AvroValue> {
    if let AvroValue::Union(_idx, inner) = value {
        if inner.as_ref() == &AvroValue::Null {
            None
        } else {
            Some(inner.as_ref())
        }
    } else {
        Some(value)
    }
}

/// Convert a vector of Avro Value records to a vector of Arrow arrays.
fn avro_records_to_arrow(
    records: &[AvroValue],
    schema: &daft_schema::schema::Schema,
    column_projection: Option<&Vec<String>>,
) -> Result<Vec<ArrayRef>> {
    let num_rows = records.len();
    let fields: Vec<_> = if let Some(cols) = column_projection {
        schema
            .fields()
            .iter()
            .filter(|f| cols.iter().any(|c| c.as_str() == f.name.as_ref()))
            .cloned()
            .collect()
    } else {
        schema.fields().to_vec()
    };

    // Extract record fields from each Value::Record into owned HashMaps
    let field_maps: Vec<HashMap<String, AvroValue>> = records
        .iter()
        .map(|r| {
            if let AvroValue::Record(fields) = r {
                fields.iter().cloned().collect()
            } else {
                HashMap::new()
            }
        })
        .collect();

    let mut arrays = Vec::with_capacity(fields.len());
    for field in &fields {
        let column_name: &str = &field.name;
        let dtype = &field.dtype;

        // Collect values for this column from each record
        let col_values: Vec<Option<&AvroValue>> = field_maps
            .iter()
            .map(|map| {
                map.get::<str>(column_name)
                    .and_then(|v| normalize_avro_value(v))
            })
            .collect();

        let array = build_arrow_array(dtype, &col_values, num_rows)?;
        arrays.push(array);
    }

    Ok(arrays)
}

/// Build an Arrow array from Avro values for a given Daft type.
fn build_arrow_array(
    dtype: &DaftDataType,
    values: &[Option<&AvroValue>],
    num_rows: usize,
) -> Result<ArrayRef> {
    match dtype {
        DaftDataType::Boolean => build_boolean_array(values, num_rows),
        DaftDataType::Int32 => build_int32_array(values, num_rows),
        DaftDataType::Int64 => build_int64_array(values, num_rows),
        DaftDataType::Float32 => build_float32_array(values, num_rows),
        DaftDataType::Float64 => build_float64_array(values, num_rows),
        DaftDataType::Utf8 => build_string_array(values, num_rows),
        DaftDataType::Binary => build_binary_array(values, num_rows),
        _ => Err(AvroError::UnsupportedType {
            type_name: format!("{:?}", dtype),
        }),
    }
}

fn build_boolean_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::new();
    for val in values.iter().take(num_rows) {
        match val {
        match val {
            Some(AvroValue::Boolean(b)) => builder.append_value(*b),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
}

fn build_int32_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Int32Builder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::Int(i)) => builder.append_value(*i),
            Some(AvroValue::Long(i)) => builder.append_value(*i as i32),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int64_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Int64Builder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::Int(i)) => builder.append_value(i64::from(*i)),
            Some(AvroValue::Long(i)) => builder.append_value(*i),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float32_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Float32Builder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::Float(f)) => builder.append_value(*f),
            Some(AvroValue::Double(d)) => builder.append_value(*d as f32),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = Float64Builder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::Float(f)) => builder.append_value(f64::from(*f)),
            Some(AvroValue::Double(d)) => builder.append_value(*d),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_string_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = StringBuilder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::String(s)) => builder.append_value(s),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_binary_array(values: &[Option<&AvroValue>], num_rows: usize) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::new();
    for val in values.iter().take(num_rows) {
        match val {
            Some(AvroValue::Bytes(b)) => builder.append_value(b),
            Some(AvroValue::Fixed(_, data)) => builder.append_value(data),
            Some(_) => builder.append_null(),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use apache_avro::{Writer, types::Value as AvroValue};

    use super::*;

    fn write_avro_bytes(records: Vec<AvroValue>, schema: &apache_avro::Schema) -> Vec<u8> {
        let mut writer = Writer::new(schema, Vec::new());
        for record in records {
            writer.append(record).unwrap();
        }
        writer.into_inner().unwrap()
    }

    fn make_schema(fields: Vec<(&str, apache_avro::Schema)>) -> apache_avro::Schema {
        use std::collections::BTreeMap;

        use apache_avro::schema::{Name, RecordField, RecordFieldOrder, RecordSchema};
        let fields: Vec<RecordField> = fields
            .into_iter()
            .map(|(name, schema)| RecordField {
                name: name.to_string(),
                doc: None,
                aliases: None,
                default: None,
                schema,
                order: RecordFieldOrder::Ignore,
                position: 0,
                custom_attributes: BTreeMap::new(),
            })
            .collect();
        let lookup: BTreeMap<String, usize> = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        apache_avro::Schema::Record(RecordSchema {
            name: Name::new("test").unwrap(),
            aliases: None,
            doc: None,
            fields,
            lookup,
            attributes: BTreeMap::new(),
        })
    }

    fn read_from_bytes(
        bytes: &[u8],
        column_projection: Option<Vec<String>>,
        max_records: Option<usize>,
    ) -> RecordBatch {
        let reader = apache_avro::Reader::new(bytes).unwrap();
        let daft_schema = avro_schema_to_daft_schema(reader.writer_schema()).unwrap();

        let mut records: Vec<AvroValue> = Vec::new();
        for value in reader {
            if max_records.is_some_and(|max| records.len() >= max) {
                break;
            }
            records.push(value.unwrap());
        }

        if records.is_empty() {
            return RecordBatch::empty(Some(Arc::new(daft_schema)));
        }

        let arrays =
            avro_records_to_arrow(&records, &daft_schema, column_projection.as_ref()).unwrap();
        let projected_schema = if let Some(ref proj) = column_projection {
            let proj_set: std::collections::HashSet<&str> =
                proj.iter().map(|s| s.as_str()).collect();
            let fields: Vec<_> = daft_schema
                .fields()
                .iter()
                .filter(|f| proj_set.contains(f.name.as_ref()))
                .cloned()
                .collect();
            Arc::new(daft_schema::schema::Schema::new(fields))
        } else {
            Arc::new(daft_schema)
        };
        RecordBatch::from_arrow(projected_schema, arrays).unwrap()
    }

    // === Basic type reads ===

    #[test]
    fn test_basic_read_boolean() {
        let schema = make_schema(vec![("col", apache_avro::Schema::Boolean)]);
        let records = vec![
            AvroValue::Record(vec![("col".to_string(), AvroValue::Boolean(true))]),
            AvroValue::Record(vec![("col".to_string(), AvroValue::Boolean(false))]),
        ];
        let bytes = write_avro_bytes(records, &schema);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 2);
        let col = daft_recordbatch::get_column_by_name(&result, "col").unwrap();
        let bools = col.bool().unwrap();
        assert_eq!(bools.get(0), Some(true));
        assert_eq!(bools.get(1), Some(false));
    }

    #[test]
    fn test_basic_read_mixed() {
        let schema = make_schema(vec![
            ("a", apache_avro::Schema::Int),
            ("b", apache_avro::Schema::String),
        ]);
        let records = vec![AvroValue::Record(vec![
            ("a".to_string(), AvroValue::Int(42)),
            ("b".to_string(), AvroValue::String("hello".to_string())),
        ])];
        let bytes = write_avro_bytes(records, &schema);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[test]
    fn test_column_projection() {
        let schema = make_schema(vec![
            ("a", apache_avro::Schema::Int),
            ("b", apache_avro::Schema::Int),
            ("c", apache_avro::Schema::String),
        ]);
        let records = vec![AvroValue::Record(vec![
            ("a".to_string(), AvroValue::Int(1)),
            ("b".to_string(), AvroValue::Int(2)),
            ("c".to_string(), AvroValue::String("x".to_string())),
        ])];
        let bytes = write_avro_bytes(records, &schema);
        let result = read_from_bytes(&bytes, Some(vec!["a".to_string(), "c".to_string()]), None);

        assert_eq!(result.schema.fields().len(), 2);
        let names: Vec<&str> = result
            .schema
            .fields()
            .iter()
            .map(|f| f.name.as_ref())
            .collect();
        assert_eq!(names, vec!["a", "c"]);
    }

    #[test]
    fn test_max_records() {
        let schema = make_schema(vec![("a", apache_avro::Schema::Int)]);
        let records: Vec<AvroValue> = (0..5)
            .map(|i| AvroValue::Record(vec![("a".to_string(), AvroValue::Int(i))]))
            .collect();
        let bytes = write_avro_bytes(records, &schema);
        let result = read_from_bytes(&bytes, None, Some(2));

        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_empty_file() {
        let schema = make_schema(vec![("a", apache_avro::Schema::Int)]);
        let bytes = write_avro_bytes(vec![], &schema);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_nullable_values() {
        let union_schema = apache_avro::Schema::Union(
            apache_avro::schema::UnionSchema::new(vec![
                apache_avro::Schema::Null,
                apache_avro::Schema::Int,
            ])
            .unwrap(),
        );
        let schema = make_schema(vec![("x", union_schema)]);
        let records = vec![
            AvroValue::Record(vec![(
                "x".to_string(),
                AvroValue::Union(1_u32.into(), Box::new(AvroValue::Int(10))),
            )]),
            AvroValue::Record(vec![(
                "x".to_string(),
                AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null)),
            )]),
        ];
        let bytes = write_avro_bytes(records, &schema);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 2);
        let col_x = daft_recordbatch::get_column_by_name(&result, "x").unwrap();
        let ints = col_x.i32().unwrap();
        assert_eq!(ints.get(0), Some(10));
        assert_eq!(ints.get(1), None);
    }
}
