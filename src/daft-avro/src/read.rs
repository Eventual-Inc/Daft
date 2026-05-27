use std::sync::Arc;

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_avro::reader::ReaderBuilder;
use arrow_schema::Schema as ArrowSchema;
use daft_recordbatch::RecordBatch;
use futures::StreamExt;

use crate::{AvroError, Result, schema::arrow_type_to_daft_type};

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
                .map_err(|e| AvroError::ReadError {
                    message: format!("Failed to read Avro file: {}", e),
                })?
        }
        daft_io::GetResult::Stream(stream, ..) => {
            let mut data = Vec::new();
            futures::pin_mut!(stream);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| AvroError::IOError { source: e })?;
                data.extend_from_slice(&chunk);
            }
            data
        }
    };

    let cursor = std::io::Cursor::new(bytes);
    let reader = ReaderBuilder::new()
        .build(std::io::BufReader::new(cursor))
        .map_err(|e| AvroError::ReadError {
            message: format!("Failed to create Avro reader: {}", e),
        })?;

    let arrow_schema = reader.schema();

    // Collect all Arrow RecordBatches
    let mut batches: Vec<ArrowRecordBatch> = Vec::new();
    let mut total_rows = 0usize;
    for batch in reader {
        let batch = batch.map_err(|e| AvroError::ReadError {
            message: format!("Failed to read Avro record batch: {}", e),
        })?;
        let batch_rows = batch.num_rows();
        if let Some(max) = max_records {
            let remaining = max.saturating_sub(total_rows);
            if remaining == 0 {
                break;
            }
            if batch_rows > remaining {
                let sliced = batch.slice(0, remaining);
                batches.push(sliced);
                break;
            }
        }
        total_rows += batch_rows;
        batches.push(batch);
    }

    if batches.is_empty() {
        let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;
        return Ok(RecordBatch::empty(Some(Arc::new(daft_schema))));
    }

    // Concatenate batches
    let combined = arrow::compute::concat_batches(&arrow_schema, batches.iter())
        .map_err(|e| AvroError::ArrowError { source: e })?;

    // Convert to Daft RecordBatch
    arrow_batch_to_daft(&combined, &arrow_schema, column_projection.as_ref())
}

fn arrow_schema_to_daft_schema(arrow_schema: &ArrowSchema) -> Result<daft_schema::schema::Schema> {
    let fields: Vec<daft_core::datatypes::Field> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            let dtype = arrow_type_to_daft_type(f.data_type())?;
            Ok(daft_core::datatypes::Field::new(f.name().clone(), dtype))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(daft_schema::schema::Schema::new(fields))
}

fn arrow_batch_to_daft(
    batch: &ArrowRecordBatch,
    arrow_schema: &ArrowSchema,
    column_projection: Option<&Vec<String>>,
) -> Result<RecordBatch> {
    use std::collections::HashSet;

    let arrays: Vec<arrow_array::ArrayRef> = if let Some(cols) = column_projection {
        let col_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
        let indices: Vec<usize> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| col_set.contains(f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        indices.iter().map(|&i| batch.column(i).clone()).collect()
    } else {
        batch.columns().to_vec()
    };

    let daft_fields: Vec<daft_core::datatypes::Field> = if let Some(cols) = column_projection {
        let col_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();
        arrow_schema
            .fields()
            .iter()
            .filter(|f| col_set.contains(f.name().as_str()))
            .map(|f| {
                let dtype = arrow_type_to_daft_type(f.data_type())?;
                Ok(daft_core::datatypes::Field::new(f.name().clone(), dtype))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let dtype = arrow_type_to_daft_type(f.data_type())?;
                Ok(daft_core::datatypes::Field::new(f.name().clone(), dtype))
            })
            .collect::<Result<Vec<_>>>()?
    };

    let projected_schema = Arc::new(daft_schema::schema::Schema::new(daft_fields));
    Ok(RecordBatch::from_arrow(projected_schema, arrays)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int32Array, RecordBatch as ArrowRecordBatch, StringArray};
    use arrow_avro::writer::WriterBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use daft_recordbatch::RecordBatch;

    use super::*;

    /// Write an Arrow RecordBatch to Avro bytes using arrow-avro.
    fn write_arrow_batch(batch: &ArrowRecordBatch) -> Vec<u8> {
        let mut output = Vec::new();
        let mut writer: arrow_avro::writer::AvroWriter<_> =
            WriterBuilder::new((*batch.schema()).clone())
                .build(&mut output)
                .unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        output
    }

    fn read_from_bytes(
        bytes: &[u8],
        column_projection: Option<Vec<String>>,
        max_records: Option<usize>,
    ) -> RecordBatch {
        let cursor = std::io::Cursor::new(bytes);
        let reader = ReaderBuilder::new()
            .build(std::io::BufReader::new(cursor))
            .unwrap();
        let arrow_schema = reader.schema();

        let mut batches: Vec<ArrowRecordBatch> = Vec::new();
        let mut total_rows = 0usize;
        for batch in reader {
            let batch = batch.unwrap();
            let batch_rows = batch.num_rows();
            if let Some(max) = max_records {
                let remaining = max.saturating_sub(total_rows);
                if remaining == 0 {
                    break;
                }
                if batch_rows > remaining {
                    let sliced = batch.slice(0, remaining);
                    batches.push(sliced);
                    break;
                }
            }
            total_rows += batch_rows;
            batches.push(batch);
        }

        if batches.is_empty() {
            let daft_schema = arrow_schema_to_daft_schema(&arrow_schema).unwrap();
            return RecordBatch::empty(Some(Arc::new(daft_schema)));
        }

        let combined = arrow::compute::concat_batches(&arrow_schema, batches.iter()).unwrap();
        arrow_batch_to_daft(&combined, &arrow_schema, column_projection.as_ref()).unwrap()
    }

    #[test]
    fn test_basic_read_boolean() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Boolean,
            true,
        )]));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![Arc::new(BooleanArray::from(vec![Some(true), Some(false)]))],
        )
        .unwrap();
        let bytes = write_arrow_batch(&batch);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 2);
        let col = daft_recordbatch::get_column_by_name(&result, "col").unwrap();
        let bools = col.bool().unwrap();
        assert_eq!(bools.get(0), Some(true));
        assert_eq!(bools.get(1), Some(false));
    }

    #[test]
    fn test_basic_read_mixed() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();
        let bytes = write_arrow_batch(&batch);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[test]
    fn test_column_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let bytes = write_arrow_batch(&batch);
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
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4]))],
        )
        .unwrap();
        let bytes = write_arrow_batch(&batch);
        let result = read_from_bytes(&bytes, None, Some(2));

        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_empty_file() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let empty_batch = ArrowRecordBatch::new_empty(schema);
        let bytes = write_arrow_batch(&empty_batch);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_nullable_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = ArrowRecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(10), None]))],
        )
        .unwrap();
        let bytes = write_arrow_batch(&batch);
        let result = read_from_bytes(&bytes, None, None);

        assert_eq!(result.num_rows(), 2);
        let col_x = daft_recordbatch::get_column_by_name(&result, "x").unwrap();
        let ints = col_x.i32().unwrap();
        assert_eq!(ints.get(0), Some(10));
        assert_eq!(ints.get(1), None);
    }
}
