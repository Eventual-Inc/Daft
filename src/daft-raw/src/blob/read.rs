use std::sync::Arc;

use async_stream::try_stream;
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::stream::BoxStream;
use tokio::io::AsyncReadExt;

use crate::{
    blob::options::{BlobConvertOptions, BlobReadOptions},
    open::open_reader,
};

fn default_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("content", DataType::Binary),
        Field::new("size", DataType::Int64),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Microseconds, Some("Etc/UTC".to_string())),
        ),
    ]))
}

/// Stream a single blob file as a one-row `RecordBatch`.
///
/// Default output schema: `{content: Binary, size: Int64, last_modified: Timestamp(us, UTC)}`.
/// Compression is NOT applied — bytes are returned verbatim.
///
/// Projection pushdown: when `convert_options.required_columns` is `Some(list)` and `"content"`
/// is not in `list`, the file is not opened and `content = NULL` is emitted.
pub async fn stream_blob(
    uri: String,
    convert_options: BlobConvertOptions,
    read_options: BlobReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let schema: SchemaRef = convert_options
        .schema
        .clone()
        .unwrap_or_else(default_schema);
    let buffer_size = read_options.buffer_size.unwrap_or(8 * 1024 * 1024);

    let needs_content = convert_options
        .required_columns
        .as_ref()
        .is_none_or(|cols| cols.iter().any(|c| c == "content"));

    let size = convert_options.size;
    let last_modified = convert_options.last_modified;
    let limit = convert_options.limit;

    let stream = try_stream! {
        if limit == Some(0) {
            return;
        }

        let content: Option<Vec<u8>> = if needs_content {
            let mut reader = open_reader(&uri, buffer_size, false, io_client, io_stats).await?;
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await?;
            Some(bytes)
        } else {
            None
        };

        yield build_record_batch(schema.clone(), content, size, last_modified)?;
    };

    Ok(Box::pin(stream))
}

fn build_record_batch(
    schema: SchemaRef,
    content: Option<Vec<u8>>,
    size: Option<i64>,
    last_modified: Option<jiff::Timestamp>,
) -> DaftResult<RecordBatch> {
    let mut columns: Vec<Series> = Vec::with_capacity(schema.len());
    for field in schema.as_ref() {
        let series = match field.name.as_ref() {
            "content" => {
                BinaryArray::from_iter("content", std::iter::once(content.as_deref())).into_series()
            }
            "size" => {
                Int64Array::from_iter(Field::new("size", DataType::Int64), std::iter::once(size))
                    .into_series()
            }
            "last_modified" => {
                let micros = last_modified.map(|ts| ts.as_microsecond());
                let physical = Int64Array::from_iter(
                    Field::new("last_modified", DataType::Int64),
                    std::iter::once(micros),
                );
                TimestampArray::new(
                    Field::new(
                        "last_modified",
                        DataType::Timestamp(TimeUnit::Microseconds, Some("Etc/UTC".to_string())),
                    ),
                    physical,
                )
                .into_series()
            }
            other => {
                return Err(common_error::DaftError::ValueError(format!(
                    "Unexpected column in blob schema: {other}"
                )));
            }
        };
        columns.push(series);
    }
    RecordBatch::new_with_size(schema, columns, 1)
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use daft_io::{IOConfig, get_io_client};
    use futures::StreamExt;

    use super::*;

    fn unique_temp_path() -> std::path::PathBuf {
        use std::{
            sync::atomic::{AtomicU64, Ordering},
            time::{SystemTime, UNIX_EPOCH},
        };
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        path.push(format!("daft_raw_blob_test_{unique}_{counter}.bin"));
        path
    }

    #[tokio::test]
    async fn test_stream_blob_basic_bytes() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let content = b"hello\x00world\xff\xfe";
        let path = unique_temp_path();
        fs::write(&path, content).unwrap();

        let stream = stream_blob(
            path.to_string_lossy().to_string(),
            BlobConvertOptions {
                size: Some(content.len() as i64),
                ..Default::default()
            },
            BlobReadOptions::default(),
            io_client,
            None,
        )
        .await
        .expect("stream_blob should succeed");

        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);
        let batch = batches.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.len(), 1);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_stream_blob_empty_file() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let path = unique_temp_path();
        fs::write(&path, b"").unwrap();

        let stream = stream_blob(
            path.to_string_lossy().to_string(),
            BlobConvertOptions {
                size: Some(0),
                ..Default::default()
            },
            BlobReadOptions::default(),
            io_client,
            None,
        )
        .await
        .expect("stream_blob should succeed on empty file");

        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_stream_blob_limit_zero() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let path = unique_temp_path();
        fs::write(&path, b"x").unwrap();

        let stream = stream_blob(
            path.to_string_lossy().to_string(),
            BlobConvertOptions {
                limit: Some(0),
                ..Default::default()
            },
            BlobReadOptions::default(),
            io_client,
            None,
        )
        .await
        .expect("stream_blob should succeed with limit=0");

        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 0);

        let _ = fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_stream_blob_skip_content_when_not_projected() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        // Use a non-existent path; if content were read, open_reader would error.
        let missing = "/this/path/does/not/exist/blob.bin".to_string();

        let stream = stream_blob(
            missing,
            BlobConvertOptions {
                size: Some(12345),
                required_columns: Some(vec!["size".to_string()]),
                ..Default::default()
            },
            BlobReadOptions::default(),
            io_client,
            None,
        )
        .await
        .expect("stream_blob should succeed when content is not projected");

        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 1);
        assert!(batches[0].is_ok());
    }
}
