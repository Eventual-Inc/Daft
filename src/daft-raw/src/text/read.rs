use std::sync::Arc;

use async_stream::try_stream;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, stream::BoxStream};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::{
    open::open_reader,
    text::options::{TextConvertOptions, TextReadOptions},
};

const READ_BLOB_HINT: &str =
    "For non-UTF-8 or arbitrary binary data, use `daft.read_blob()` instead.";

fn map_decode_err(e: std::io::Error) -> DaftError {
    if e.kind() == std::io::ErrorKind::InvalidData {
        DaftError::ValueError(format!(
            "Failed to decode file as UTF-8: {e}. {READ_BLOB_HINT}"
        ))
    } else {
        e.into()
    }
}

/// Stream text lines from a URI into `RecordBatch` chunks with a single Utf8 column named "content".
///
/// The `encoding` argument is currently restricted to UTF-8 (case-insensitive). Any other encoding
/// will result in a `ValueError`.
pub async fn stream_text(
    uri: String,
    convert_options: TextConvertOptions,
    read_options: TextReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    // Only support UTF-8-compatible encodings
    let encoding = convert_options.encoding.as_str();
    if !encoding.eq_ignore_ascii_case("utf-8") && !encoding.eq_ignore_ascii_case("utf8") {
        return Err(DaftError::ValueError(format!(
            "Unsupported text encoding: {encoding}. Only UTF-8 is currently supported. {READ_BLOB_HINT}",
        )));
    }

    // Schema for the output RecordBatches, default is a single UTF8 column named "content".
    let schema: SchemaRef = convert_options
        .schema
        .clone()
        .unwrap_or_else(|| Arc::new(Schema::new(vec![Field::new("content", DataType::Utf8)])));

    // Check if we're reading the whole file as a single row
    if convert_options.whole_text {
        let whole_text_stream =
            read_into_whole_text_stream(uri, convert_options, read_options, io_client, io_stats)
                .await?;
        let out: BoxStream<'static, DaftResult<RecordBatch>> =
            Box::pin(whole_text_stream.map(move |content_res| {
                let content = content_res?;
                let array = Utf8Array::from_values("content", std::iter::once(content.as_str()));
                let series = array.into_series();
                RecordBatch::new_with_size(schema.clone(), vec![series], 1)
            }));
        return Ok(out);
    }

    // Build a stream of line chunks
    let line_chunk_stream =
        read_into_line_chunk_stream(uri, convert_options, read_options, io_client, io_stats)
            .await?;

    let table_stream = line_chunk_stream.map(move |chunk_res| {
        let lines = chunk_res?;
        let row_count = lines.len();
        if row_count == 0 {
            return Ok(RecordBatch::empty(Some(schema.clone())));
        }

        let array = Utf8Array::from_values("content", lines.iter().map(|s| s.as_str()));
        let series = array.into_series();
        RecordBatch::new_with_size(schema.clone(), vec![series], row_count)
    });

    let out: BoxStream<'static, DaftResult<RecordBatch>> = Box::pin(table_stream);
    Ok(out)
}

async fn read_into_whole_text_stream(
    uri: String,
    convert_options: TextConvertOptions,
    read_options: TextReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<impl Stream<Item = DaftResult<String>> + Send> {
    let buffer_size = read_options.buffer_size.unwrap_or(8 * 1024 * 1024);
    let mut reader = open_reader(&uri, buffer_size, true, io_client, io_stats).await?;

    Ok(try_stream! {
        // Check limit first, and skip read if limit is 0
        if convert_options.limit == Some(0) {
            return;
        }

        let mut content = String::new();
        reader.read_to_string(&mut content).await.map_err(map_decode_err)?;

        // Apply skip_blank_lines if needed (for whole file, this means skip if entire content is blank)
        if convert_options.skip_blank_lines && content.trim().is_empty() {
            return;
        }

        yield content;
    })
}

async fn read_into_line_chunk_stream(
    uri: String,
    convert_options: TextConvertOptions,
    read_options: TextReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<impl Stream<Item = DaftResult<Vec<String>>> + Send> {
    let buffer_size = read_options.buffer_size.unwrap_or(8 * 1024 * 1024);
    let chunk_size = read_options.chunk_size.unwrap_or(64 * 1024);
    let reader = open_reader(&uri, buffer_size, true, io_client, io_stats).await?;

    let line_stream = tokio_stream::wrappers::LinesStream::new(reader.lines());
    Ok(try_stream! {
        let mut stream = line_stream;
        let mut chunk: Vec<String> = Vec::with_capacity(chunk_size);

        let skip_blank_lines = convert_options.skip_blank_lines;
        let mut remaining = convert_options.limit.unwrap_or(usize::MAX);

        while remaining > 0 {
            match stream.next().await {
                Some(line_res) => {
                    let line = line_res.map_err(map_decode_err)?;
                    if skip_blank_lines && line.trim().is_empty() {
                        continue;
                    }
                    remaining -= 1;
                    chunk.push(line);
                    if chunk.len() >= chunk_size {
                        let to_yield = std::mem::replace(&mut chunk, Vec::with_capacity(chunk_size));
                        yield to_yield;
                    }
                }
                None => {
                    break;
                }
            }
        }

        if !chunk.is_empty() {
            yield chunk;
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use daft_io::{IOConfig, get_io_client};
    use flate2::{Compression, write::GzEncoder};
    use futures::StreamExt;

    use super::*;

    fn unique_temp_path(extension: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
        path.push(format!("daft_raw_text_test_{unique}_{counter}.{extension}"));
        path
    }

    fn create_test_file(content: &str, compressed: bool) -> (std::path::PathBuf, String) {
        let extension = if compressed { "gz" } else { "txt" };
        let path = unique_temp_path(extension);

        if compressed {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(content.as_bytes())
                .expect("failed to compress content");
            let compressed_data = encoder.finish().expect("failed to finish compression");
            fs::write(&path, &compressed_data).expect("failed to write compressed file");
        } else {
            fs::write(&path, content).expect("failed to write temp text file");
        }

        (path, content.to_string())
    }

    #[tokio::test]
    async fn test_read_into_whole_text_stream() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let test_cases = vec![
            ("uncompressed with default buffer", false, None, None),
            ("uncompressed with small buffer", false, Some(16), None),
            (
                "uncompressed with large buffer",
                false,
                Some(1024 * 1024),
                None,
            ),
            ("gzip compressed with default buffer", true, None, None),
            ("gzip compressed with small buffer", true, Some(16), None),
            (
                "gzip compressed with large buffer",
                true,
                Some(1024 * 1024),
                None,
            ),
            ("uncompressed with limit=0", false, None, Some(0)),
            ("gzip compressed with limit=0", true, None, Some(0)),
        ];

        for (name, compressed, buffer_size, limit) in test_cases {
            let content = "Hello, World!\nThis is a test file.\nMultiple lines here.\n";
            let (path, expected_content) = create_test_file(content, compressed);

            let read_options = TextReadOptions {
                buffer_size,
                ..Default::default()
            };

            let convert_options = TextConvertOptions {
                limit,
                ..Default::default()
            };

            let stream = read_into_whole_text_stream(
                path.to_string_lossy().to_string(),
                convert_options,
                read_options,
                io_client.clone(),
                None,
            )
            .await
            .unwrap_or_else(|_| panic!("read_into_whole_text_stream should succeed for {name}"));

            let results: Vec<_> = stream.collect::<Vec<_>>().await;

            if limit == Some(0) {
                assert_eq!(
                    results.len(),
                    0,
                    "[{name}] expected zero results for limit=0"
                );
            } else {
                assert_eq!(results.len(), 1, "[{name}] expected exactly one result");

                let actual_content = results[0]
                    .as_ref()
                    .unwrap_or_else(|_| panic!("[{name}] stream yielded error"))
                    .clone();
                assert_eq!(
                    actual_content, expected_content,
                    "[{name}] content mismatch"
                );
            }

            let _ = fs::remove_file(&path);
        }
    }

    #[tokio::test]
    async fn test_read_into_line_chunk_stream() {
        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let test_cases = vec![
            ("uncompressed with default buffer", false, None),
            ("uncompressed with small buffer", false, Some(16)),
            ("uncompressed with large buffer", false, Some(1024 * 1024)),
            ("gzip compressed with default buffer", true, None),
            ("gzip compressed with small buffer", true, Some(16)),
            ("gzip compressed with large buffer", true, Some(1024 * 1024)),
        ];

        for (name, compressed, buffer_size) in test_cases {
            let content = "line1\nline2\nline3\nline4\nline5\n";
            let (path, _) = create_test_file(content, compressed);

            let read_options = TextReadOptions {
                buffer_size,
                chunk_size: Some(2),
            };

            let stream = read_into_line_chunk_stream(
                path.to_string_lossy().to_string(),
                TextConvertOptions::default(),
                read_options,
                io_client.clone(),
                None,
            )
            .await
            .unwrap_or_else(|_| panic!("read_into_line_chunk_stream should succeed for {name}"));

            let chunks: Vec<_> = stream.collect::<Vec<_>>().await;

            let all_lines: Vec<String> = chunks
                .iter()
                .flat_map(|chunk_res| {
                    chunk_res
                        .as_ref()
                        .unwrap_or_else(|_| panic!("[{name}] stream yielded error"))
                        .clone()
                })
                .collect();

            assert_eq!(
                all_lines,
                vec!["line1", "line2", "line3", "line4", "line5"],
                "[{name}] lines mismatch"
            );

            let _ = fs::remove_file(&path);
        }
    }
}
