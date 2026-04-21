use std::sync::Arc;

use common_error::DaftResult;
use daft_compression::CompressionCodec;
use daft_io::{GetResult, IOClient, IOStatsRef};
use tokio::{
    fs::File,
    io::{AsyncBufRead, BufReader},
};
use tokio_util::io::StreamReader;

/// Open a URI and return an async buffered reader.
///
/// When `apply_compression` is true and the URI has a recognized compression extension
/// (e.g., `.gz`), the returned reader transparently decompresses. When false, bytes are
/// returned verbatim — callers that need raw bytes (e.g., `read_blob`) should pass false.
pub(crate) async fn open_reader(
    uri: &str,
    buffer_size: usize,
    apply_compression: bool,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Box<dyn AsyncBufRead + Unpin + Send>> {
    let reader: Box<dyn AsyncBufRead + Unpin + Send> = match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => Box::new(BufReader::with_capacity(
            buffer_size,
            File::open(file.path).await?,
        )),
        GetResult::Stream(stream, ..) => Box::new(BufReader::with_capacity(
            buffer_size,
            StreamReader::new(stream),
        )),
    };
    if !apply_compression {
        return Ok(reader);
    }
    Ok(match CompressionCodec::from_uri(uri) {
        Some(codec) => Box::new(BufReader::with_capacity(
            buffer_size,
            codec.to_decoder(reader),
        )),
        None => reader,
    })
}
