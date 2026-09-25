use std::sync::Arc;

use arrow_array::Array;
use arrow_avro::{compression::CompressionCodec, writer::WriterBuilder};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use daft_recordbatch::{RecordBatch, get_column_by_name};

use crate::{AvroCompression, AvroError, AvroWriteOptions, Result};

/// Map an Arrow DataType to its Avro-compatible signed equivalent.
/// Avro only supports signed integer types (int=Int32, long=Int64).
fn arrow_type_for_avro(dt: &ArrowDataType) -> ArrowDataType {
    match dt {
        ArrowDataType::UInt8 | ArrowDataType::UInt16 => ArrowDataType::Int32,
        ArrowDataType::UInt32 | ArrowDataType::UInt64 => ArrowDataType::Int64,
        _ => dt.clone(),
    }
}

/// Convert an Arrow schema, replacing unsigned integer types with signed equivalents
/// that Avro can represent.
fn convert_arrow_schema_for_avro(arrow_schema: &ArrowSchema) -> ArrowSchema {
    let new_fields: Vec<ArrowField> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let new_type = arrow_type_for_avro(field.data_type());
            if &new_type != field.data_type() {
                ArrowField::new(field.name(), new_type, field.is_nullable())
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    ArrowSchema::new(new_fields)
}

/// Cast an Arrow array to the target type, with overflow validation for UInt64.
fn cast_array_for_avro(
    array: &dyn Array,
    target_type: &ArrowDataType,
) -> Result<arrow_array::ArrayRef> {
    // Validate UInt64 values fit in Avro Long (i64)
    if *array.data_type() == ArrowDataType::UInt64 {
        let arr = array
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        for val in arr.iter().flatten() {
            if val > i64::MAX as u64 {
                return Err(AvroError::WriteError {
                    message: format!(
                        "Column of type UInt64 contains value {} which exceeds Avro Long max ({}). \
                         Avro only supports signed 64-bit integers.",
                        val,
                        i64::MAX,
                    ),
                });
            }
        }
    }

    let source_type = array.data_type();
    arrow::compute::cast(array, target_type).map_err(|e| AvroError::WriteError {
        message: format!(
            "Failed to cast array from {} to {}: {}",
            source_type, target_type, e
        ),
    })
}

/// Write a RecordBatch to an Avro file buffer.
///
/// Returns the serialized bytes of the Avro Object Container File.
pub fn write_record_batch_to_avro(
    record_batch: &RecordBatch,
    options: &AvroWriteOptions,
) -> Result<Vec<u8>> {
    let daft_schema = record_batch.schema.clone();
    let num_rows = record_batch.num_rows();

    // Convert Daft schema to Arrow schema
    let arrow_schema =
        Arc::new(
            daft_schema
                .to_arrow()
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!("Failed to convert Daft schema to Arrow schema: {}", e),
                })?,
        );

    // Convert schema to Avro-compatible types (unsigned → signed)
    let avro_schema = Arc::new(convert_arrow_schema_for_avro(&arrow_schema));

    let compression: Option<CompressionCodec> = match options.compression {
        AvroCompression::Null => None,
        AvroCompression::Deflate => Some(CompressionCodec::Deflate),
    };

    let mut output = Vec::new();

    if num_rows == 0 {
        let builder =
            WriterBuilder::new(avro_schema.as_ref().clone()).with_compression(compression);
        let mut writer: arrow_avro::writer::AvroWriter<_> =
            builder
                .build(&mut output)
                .map_err(|e| AvroError::WriteError {
                    message: format!("Failed to create Avro writer: {}", e),
                })?;
        let empty_batch = arrow_array::RecordBatch::new_empty(avro_schema);
        writer
            .write(&empty_batch)
            .map_err(|e| AvroError::WriteError {
                message: format!("Failed to write empty Avro batch: {}", e),
            })?;
        writer.finish().map_err(|e| AvroError::WriteError {
            message: format!("Failed to finalize Avro writer: {}", e),
        })?;
        drop(writer);
    } else {
        // Convert Daft columns to Arrow arrays, casting unsigned ints to signed
        let mut arrow_arrays = Vec::with_capacity(daft_schema.len());
        for (i, field) in daft_schema.fields().iter().enumerate() {
            let column = get_column_by_name(record_batch, &field.name)?;
            let arrow_array = column
                .to_arrow()
                .map_err(|e| AvroError::SchemaConversionError {
                    message: format!(
                        "Failed to convert column '{}' to Arrow array: {}",
                        field.name, e
                    ),
                })?;
            let target_type = avro_schema.field(i).data_type();
            let casted_array = cast_array_for_avro(arrow_array.as_ref(), target_type)?;
            arrow_arrays.push(casted_array);
        }

        let arrow_batch = arrow_array::RecordBatch::try_new(avro_schema.clone(), arrow_arrays)
            .map_err(|e| AvroError::WriteError {
                message: format!("Failed to create Arrow RecordBatch: {}", e),
            })?;

        let builder =
            WriterBuilder::new(avro_schema.as_ref().clone()).with_compression(compression);
        let mut writer: arrow_avro::writer::AvroWriter<_> =
            builder
                .build(&mut output)
                .map_err(|e| AvroError::WriteError {
                    message: format!("Failed to create Avro writer: {}", e),
                })?;

        writer
            .write(&arrow_batch)
            .map_err(|e| AvroError::WriteError {
                message: format!("Failed to write Avro batch: {}", e),
            })?;
        writer.finish().map_err(|e| AvroError::WriteError {
            message: format!("Failed to finalize Avro writer: {}", e),
        })?;
        drop(writer);
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, Int64Array, StringArray, UInt64Array};
    use arrow_avro::reader::ReaderBuilder;
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

        // Verify that the output is valid Avro by reading it back with arrow-avro
        let cursor = std::io::Cursor::new(bytes);
        let reader = ReaderBuilder::new()
            .build(std::io::BufReader::new(cursor))
            .unwrap();
        let arrow_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(!arrow_batches.is_empty());
        let total_rows: usize = arrow_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_write_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32)]));
        let empty = RecordBatch::empty(Some(schema));
        let bytes = write_record_batch_to_avro(&empty, &AvroWriteOptions::default()).unwrap();
        assert!(!bytes.is_empty());

        let cursor = std::io::Cursor::new(bytes);
        let reader = ReaderBuilder::new()
            .build(std::io::BufReader::new(cursor))
            .unwrap();
        let arrow_schema = reader.schema();
        assert_eq!(arrow_schema.fields().len(), 1);
        assert_eq!(arrow_schema.field(0).name().as_str(), "x");
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
        let cursor = std::io::Cursor::new(bytes);
        let reader = ReaderBuilder::new()
            .build(std::io::BufReader::new(cursor))
            .unwrap();
        let arrow_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = arrow_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
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
