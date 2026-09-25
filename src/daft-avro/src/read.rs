use std::sync::Arc;

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_avro::{
    errors::AvroError as ArrowAvroError,
    reader::{AsyncFileReader, ReaderBuilder},
};
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use daft_io::GetRange;
use daft_recordbatch::RecordBatch;
use futures::StreamExt;

use crate::{AvroError, Result};

/// Wraps `daft_io::IOClient` to implement arrow_avro's `AsyncFileReader` trait
/// for range-based remote reads.
struct RemoteAvroReader {
    uri: String,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
}

impl AsyncFileReader for RemoteAvroReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'_, Result<Bytes, ArrowAvroError>> {
        Box::pin(async move {
            let get_result = self
                .io_client
                .single_url_get(
                    self.uri.clone(),
                    Some(GetRange::Bounded(range.start as usize..range.end as usize)),
                    self.io_stats.clone(),
                )
                .await
                .map_err(|e| ArrowAvroError::External(Box::new(e)))?;
            get_result
                .bytes()
                .await
                .map_err(|e| ArrowAvroError::External(Box::new(e)))
        })
    }
}

#[cfg(test)]
fn read_avro_from_bytes(
    bytes: Vec<u8>,
    column_projection: Option<&Vec<String>>,
    max_records: Option<usize>,
) -> Result<RecordBatch> {
    let cursor = std::io::Cursor::new(bytes);
    let reader = ReaderBuilder::new()
        .build(std::io::BufReader::new(cursor))
        .map_err(|e| AvroError::ReadError {
            message: format!("Failed to create Avro reader: {}", e),
        })?;
    let arrow_schema = reader.schema();
    drain_reader(reader, &arrow_schema, column_projection, max_records)
}

/// Read an Avro file into a RecordBatch.
/// Local files use BufReader<File> via spawn_blocking;
/// remote streams use AsyncAvroFileReader with range-based reads.
pub async fn read_avro(
    uri: &str,
    io_client: Arc<daft_io::IOClient>,
    io_stats: Option<daft_io::IOStatsRef>,
    column_projection: Option<Vec<String>>,
    max_records: Option<usize>,
) -> Result<RecordBatch> {
    let get_result = io_client
        .single_url_get(uri.to_string(), None, io_stats.clone())
        .await
        .map_err(|e| AvroError::IOError { source: e })?;

    match get_result {
        daft_io::GetResult::File(file) => {
            let path = file.path.clone();
            tokio::task::spawn_blocking(move || {
                let file = std::fs::File::open(&path).map_err(|e| AvroError::ReadError {
                    message: format!("Failed to open Avro file {}: {}", path.display(), e),
                })?;
                let reader = ReaderBuilder::new()
                    .build(std::io::BufReader::new(file))
                    .map_err(|e| AvroError::ReadError {
                        message: format!("Failed to create Avro reader: {}", e),
                    })?;
                let arrow_schema = reader.schema();
                drain_reader(
                    reader,
                    &arrow_schema,
                    column_projection.as_ref(),
                    max_records,
                )
            })
            .await
            .map_err(|e| AvroError::ReadError {
                message: format!("Failed to join blocking task: {}", e),
            })?
        }
        daft_io::GetResult::Stream(stream, ..) => {
            // Drop the full-file download; use range-based reads instead.
            drop(stream);

            let file_size = io_client
                .single_url_get_size(uri.to_string(), io_stats.clone())
                .await
                .map_err(|e| AvroError::IOError { source: e })? as u64;

            let remote = RemoteAvroReader {
                uri: uri.to_string(),
                io_client,
                io_stats,
            };

            let mut avro_reader =
                arrow_avro::reader::AsyncAvroFileReader::builder(remote, file_size, 1024)
                    .try_build()
                    .await
                    .map_err(|e| AvroError::ReadError {
                        message: format!("Failed to create async Avro reader: {}", e),
                    })?;

            let arrow_schema = avro_reader.schema();
            let mut batches: Vec<ArrowRecordBatch> = Vec::new();
            let mut total_rows = 0usize;
            while let Some(batch) = avro_reader.next().await {
                let batch = batch.map_err(|e| AvroError::ArrowError { source: e })?;
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

            finish_batches(batches, &arrow_schema, column_projection.as_ref())
        }
    }
}

fn drain_reader<R: std::io::BufRead>(
    reader: arrow_avro::reader::Reader<R>,
    arrow_schema: &Arc<ArrowSchema>,
    column_projection: Option<&Vec<String>>,
    max_records: Option<usize>,
) -> Result<RecordBatch> {
    let mut batches: Vec<ArrowRecordBatch> = Vec::new();
    let mut total_rows = 0usize;
    for batch in reader {
        let batch = batch.map_err(|e| AvroError::ArrowError { source: e })?;
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

    finish_batches(batches, arrow_schema, column_projection)
}

fn finish_batches(
    batches: Vec<ArrowRecordBatch>,
    arrow_schema: &Arc<ArrowSchema>,
    column_projection: Option<&Vec<String>>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        let daft_schema =
            daft_schema::schema::Schema::try_from(arrow_schema.as_ref()).map_err(|e| {
                AvroError::SchemaConversionError {
                    message: format!("Failed to convert Arrow schema: {}", e),
                }
            })?;
        return Ok(RecordBatch::empty(Some(Arc::new(daft_schema))));
    }

    let combined = arrow::compute::concat_batches(arrow_schema, batches.iter())
        .map_err(|e| AvroError::ArrowError { source: e })?;

    arrow_batch_to_daft(&combined, arrow_schema, column_projection)
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
                f.as_ref()
                    .try_into()
                    .map_err(|e| AvroError::SchemaConversionError {
                        message: format!("Failed to convert Arrow field: {}", e),
                    })
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        arrow_schema
            .fields()
            .iter()
            .map(|f| {
                f.as_ref()
                    .try_into()
                    .map_err(|e| AvroError::SchemaConversionError {
                        message: format!("Failed to convert Arrow field: {}", e),
                    })
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
        read_avro_from_bytes(bytes.to_vec(), column_projection.as_ref(), max_records).unwrap()
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
