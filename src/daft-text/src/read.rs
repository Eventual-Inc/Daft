use std::sync::Arc;

use async_stream::try_stream;
use common_error::{DaftError, DaftResult};
use daft_compression::CompressionCodec;
use daft_core::prelude::*;
use daft_io::{GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, stream::BoxStream};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
};
use tokio_util::io::StreamReader;

use crate::options::{TextConvertOptions, TextReadOptions};

/// Stream text lines from a URI into `RecordBatch` chunks with a single Utf8 column named "text".
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
            "Unsupported text encoding: {encoding}. Only UTF-8 is currently supported.",
        )));
    }

    // Schema for the output RecordBatches, default is a single UTF8 column named "text".
    let schema: SchemaRef = convert_options
        .schema
        .clone()
        .unwrap_or_else(|| Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8)])));

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

        let array = Utf8Array::from_values("text", lines.iter().map(|s| s.as_str()));
        let series = array.into_series();
        RecordBatch::new_with_size(schema.clone(), vec![series], row_count)
    });

    Ok(Box::pin(table_stream))
}

async fn read_into_line_chunk_stream(
    uri: String,
    convert_options: TextConvertOptions,
    read_options: TextReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<impl Stream<Item = DaftResult<Vec<String>>> + Send> {
    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.clone(), None, io_stats)
            .await?
        {
            GetResult::File(file) => {
                // Use user-provided buffer size, otherwise falling back to 256KiB as the default.
                let buffer_size = read_options.buffer_size.unwrap_or(256 * 1024);
                let chunk_size = read_options.chunk_size.unwrap_or(64 * 1024);
                (
                    Box::new(BufReader::with_capacity(
                        buffer_size,
                        File::open(file.path).await?,
                    )),
                    buffer_size,
                    chunk_size,
                )
            }
            GetResult::Stream(stream, ..) => {
                // Use user-provided buffer size, otherwise falling back to 8MiB as the default.
                let buffer_size = read_options.buffer_size.unwrap_or(8 * 1024 * 1024);
                let chunk_size = read_options.chunk_size.unwrap_or(64 * 1024);
                (Box::new(StreamReader::new(stream)), buffer_size, chunk_size)
            }
        };

    // If file is compressed, wrap stream in decoding stream.
    let reader: Box<dyn AsyncBufRead + Unpin + Send> = match CompressionCodec::from_uri(&uri) {
        Some(compression) => Box::new(BufReader::with_capacity(
            buffer_size,
            compression.to_decoder(reader),
        )),
        None => reader,
    };

    let line_stream = tokio_stream::wrappers::LinesStream::new(reader.lines());
    Ok(try_stream! {
        let mut stream = line_stream;
        let mut chunk: Vec<String> = Vec::with_capacity(chunk_size);

        let skip_blank_lines = convert_options.skip_blank_lines;
        let mut remaining = convert_options.limit.unwrap_or(usize::MAX);

        while remaining > 0 {
            match stream.next().await {
                Some(line_res) => {
                    let line = line_res?;
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
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use daft_io::{IOConfig, get_io_client};
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn read_local_text_file() {
        // Create a uniquely named temporary file in the system temp directory.
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_nanos();
        path.push(format!("daft_text_stream_test_{unique}.txt"));

        fs::write(&path, b"line1\nline2\n").expect("failed to write temp text file");

        let io_config = Arc::new(IOConfig::default());
        let io_client = get_io_client(false, io_config).expect("failed to construct IOClient");

        let stream = stream_text(
            path.to_string_lossy().to_string(),
            TextConvertOptions::default(),
            TextReadOptions::default(),
            io_client,
            None,
        )
        .await
        .expect("stream_text should succeed for local file");

        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        assert!(!batches.is_empty(), "expected at least one RecordBatch");

        let total_rows: usize = batches
            .into_iter()
            .map(|res| res.expect("stream yielded error RecordBatch"))
            .map(|rb| rb.num_rows())
            .sum();
        assert_eq!(total_rows, 2);

        let _ = fs::remove_file(&path);
    }
}
