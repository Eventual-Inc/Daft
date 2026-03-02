use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::{BinaryArray, IntoSeries, Schema, SchemaRef};
use daft_io::{GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::stream::BoxStream;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, BufReader},
};
use tokio_util::io::StreamReader;

use crate::options::{BinaryConvertOptions, BinaryReadOptions};

const LOCAL_DEFAULT_BUFFER_SIZE: usize = 256 * 1024;
const REMOTE_DEFAULT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

async fn read_all_bytes(
    mut reader: impl AsyncRead + Unpin,
    max_bytes: Option<usize>,
) -> DaftResult<Vec<u8>> {
    let mut buf = Vec::new();

    if let Some(max_bytes) = max_bytes {
        let mut chunk = [0u8; 8192];
        loop {
            let n = reader.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            if buf.len() + n > max_bytes {
                return Err(DaftError::ValueError(format!(
                    "Binary file exceeds max_bytes={max_bytes}"
                )));
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    } else {
        reader.read_to_end(&mut buf).await?;
    }

    Ok(buf)
}

#[allow(clippy::too_many_arguments)]
pub async fn stream_binary(
    uri: String,
    schema: SchemaRef,
    convert_options: BinaryConvertOptions,
    read_options: BinaryReadOptions,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    if matches!(convert_options.limit, Some(0)) {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let schema = if convert_options.include_bytes {
        schema
    } else {
        Arc::new(Schema::empty())
    };

    Ok(Box::pin(futures::stream::once(async move {
        if !convert_options.include_bytes {
            return RecordBatch::new_with_size(schema, vec![], 1);
        }

        let get_result = io_client.single_url_get(uri, None, io_stats).await?;

        let (buffer_size, maybe_len, local_file) = match &get_result {
            GetResult::File(file) => (
                read_options
                    .buffer_size
                    .unwrap_or(LOCAL_DEFAULT_BUFFER_SIZE),
                None,
                Some(file.path.clone()),
            ),
            GetResult::Stream(_, size, ..) => (
                read_options
                    .buffer_size
                    .unwrap_or(REMOTE_DEFAULT_BUFFER_SIZE),
                *size,
                None,
            ),
        };

        if let Some(max_bytes) = read_options.max_bytes {
            if let Some(len) = maybe_len
                && len > max_bytes
            {
                return Err(DaftError::ValueError(format!(
                    "Binary file exceeds max_bytes={max_bytes}"
                )));
            }
            if let Some(path) = local_file {
                let len = tokio::fs::metadata(&path).await?.len() as usize;
                if len > max_bytes {
                    return Err(DaftError::ValueError(format!(
                        "Binary file exceeds max_bytes={max_bytes}"
                    )));
                }
            }
        }

        let reader: Box<dyn AsyncRead + Unpin + Send> = match get_result {
            GetResult::File(file) => Box::new(BufReader::with_capacity(
                buffer_size,
                File::open(file.path).await?,
            )),
            GetResult::Stream(stream, ..) => Box::new(BufReader::with_capacity(
                buffer_size,
                StreamReader::new(stream),
            )),
        };

        let bytes = read_all_bytes(reader, read_options.max_bytes).await?;
        let bytes_series =
            BinaryArray::from_iter("bytes", std::iter::once(Some(bytes.as_slice()))).into_series();

        RecordBatch::new_with_size(schema, vec![bytes_series], 1)
    })))
}
