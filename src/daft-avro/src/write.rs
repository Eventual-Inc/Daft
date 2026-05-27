use apache_avro::{Writer, types::Value as AvroValue};
use daft_core::{datatypes::DataType as DaftDataType, series::Series};
use daft_recordbatch::{RecordBatch, get_column_by_name};

use crate::{AvroError, AvroWriteOptions, Result, schema::daft_schema_to_avro_schema};

/// Write a RecordBatch to an Avro file buffer.
///
/// Returns the serialized bytes of the Avro Object Container File.
pub fn write_record_batch_to_avro(
    record_batch: &RecordBatch,
    options: &AvroWriteOptions,
) -> Result<Vec<u8>> {
    let daft_schema = record_batch.schema.clone();
    let num_rows = record_batch.num_rows();

    let avro_schema = daft_schema_to_avro_schema(
        &daft_schema,
        &options.record_name,
        &options.record_namespace,
    )?;

    if num_rows == 0 {
        // Write an empty Avro file with just the schema
        let codec = options.compression.to_avro_codec();
        let writer = Writer::with_codec(&avro_schema, Vec::new(), codec);
        writer.into_inner().map_err(|e| AvroError::WriteError {
            source: Box::new(e),
        })
    } else {
        let codec = options.compression.to_avro_codec();
        let mut writer = Writer::with_codec(&avro_schema, Vec::new(), codec);

        // Convert each row to an Avro Record
        let fields = daft_schema.fields();
        for row_idx in 0..num_rows {
            let mut record_fields = Vec::with_capacity(fields.len());
            for field in fields {
                let column = get_column_by_name(record_batch, &field.name)?;
                let avro_value = daft_element_to_avro_value(column, row_idx, &field.dtype)?;
                record_fields.push((field.name.to_string(), avro_value));
            }
            writer
                .append(AvroValue::Record(record_fields))
                .map_err(|e| AvroError::WriteError {
                    source: Box::new(e),
                })?;
        }

        writer.into_inner().map_err(|e| AvroError::WriteError {
            source: Box::new(e),
        })
    }
}

/// Convert a single element from a Daft Series to an Avro Value.
fn daft_element_to_avro_value(
    series: &Series,
    index: usize,
    dtype: &DaftDataType,
) -> Result<AvroValue> {
    // All fields are nullable in the Avro schema (Union[Null, T]).
    // For non-null values, wrap in Union(1, value); for null, Union(0, Null).
    // For Null-typed columns, just emit Null.
    let inner = match dtype {
        DaftDataType::Null => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        DaftDataType::Boolean => match series.bool()?.get(index) {
            Some(v) => AvroValue::Boolean(v),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Int8 => match series.i8()?.get(index) {
            Some(v) => AvroValue::Int(v.into()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Int16 => match series.i16()?.get(index) {
            Some(v) => AvroValue::Int(v.into()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Int32 => match series.i32()?.get(index) {
            Some(v) => AvroValue::Int(v),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Int64 => match series.i64()?.get(index) {
            Some(v) => AvroValue::Long(v),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::UInt8 => match series.u8()?.get(index) {
            Some(v) => AvroValue::Int(v.into()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::UInt16 => match series.u16()?.get(index) {
            Some(v) => AvroValue::Int(v.into()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::UInt32 => match series.u32()?.get(index) {
            Some(v) => AvroValue::Long(v.into()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::UInt64 => match series.u64()?.get(index) {
            Some(v) => {
                let v = i64::try_from(v).map_err(|_| AvroError::SchemaConversionError {
                    message: format!("UInt64 value {} exceeds Avro Long (i64) range", v),
                })?;
                AvroValue::Long(v)
            }
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Float32 => match series.f32()?.get(index) {
            Some(v) => AvroValue::Float(v),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Float64 => match series.f64()?.get(index) {
            Some(v) => AvroValue::Double(v),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Utf8 => match series.utf8()?.get(index) {
            Some(v) => AvroValue::String(v.to_string()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        DaftDataType::Binary => match series.binary()?.get(index) {
            Some(v) => AvroValue::Bytes(v.to_vec()),
            None => return Ok(AvroValue::Union(0_u32.into(), Box::new(AvroValue::Null))),
        },
        _ => {
            return Err(AvroError::UnsupportedType {
                type_name: format!("{:?}", dtype),
            });
        }
    };
    // Wrap the value in Union(1, value) since schema is [Null, T]
    Ok(AvroValue::Union(1_u32.into(), Box::new(inner)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, Int64Array, StringArray, UInt64Array};
    use daft_core::prelude::*;
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn make_batch(columns: Vec<(&str, DataType, arrow_array::ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, dtype, _)| Field::new(*name, dtype.clone()))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<arrow_array::ArrayRef> =
            columns.into_iter().map(|(_, _, arr)| arr).collect();
        RecordBatch::from_arrow(schema, arrays).unwrap()
    }

    #[test]
    fn test_write_basic_types() {
        let batch = make_batch(vec![
            (
                "a",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ),
            (
                "b",
                DataType::Float64,
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ),
            (
                "c",
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ),
        ]);
        let bytes = write_record_batch_to_avro(&batch, &AvroWriteOptions::default()).unwrap();
        assert!(!bytes.is_empty());

        // Verify that the output is valid Avro by reading it back
        let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
        let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_write_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32)]));
        let empty = RecordBatch::empty(Some(schema));
        let bytes = write_record_batch_to_avro(&empty, &AvroWriteOptions::default()).unwrap();
        assert!(!bytes.is_empty());

        // Empty file should still be valid Avro with schema
        let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
        let ws = reader.writer_schema();
        if let apache_avro::Schema::Record(rs) = ws {
            assert_eq!(rs.fields.len(), 1);
            assert_eq!(rs.fields[0].name, "x");
        } else {
            panic!("expected Record schema");
        }
    }

    #[test]
    fn test_write_deflate_compression() {
        let batch = make_batch(vec![(
            "a",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        )]);
        let options = AvroWriteOptions::new(
            crate::AvroCompression::Deflate,
            "test".to_string(),
            String::new(),
        );
        let bytes = write_record_batch_to_avro(&batch, &options).unwrap();
        assert!(!bytes.is_empty());
        // Should still be valid
        let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
        let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_write_uint64_within_range() {
        let batch = make_batch(vec![(
            "a",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![i64::MAX as u64, 0])),
        )]);
        let result = write_record_batch_to_avro(&batch, &AvroWriteOptions::default()).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_write_uint64_overflow_error() {
        let overflow_val = i64::MAX as u64 + 1;
        let batch = make_batch(vec![(
            "a",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![overflow_val])),
        )]);
        let result = write_record_batch_to_avro(&batch, &AvroWriteOptions::default());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds Avro Long"), "got: {}", err);
    }
}
