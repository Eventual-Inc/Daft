#![feature(async_closure)]

use daft_io::object_io::GetResult;
use futures::{AsyncRead, StreamExt, TryStreamExt};

struct SeekableReader {
    uri: String,
    pos: i64,
}

impl AsyncRead for SeekableReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let io_config = daft_io::IOConfig::default();
        let get_result = daft_io::single_url_get(self.uri, &io_config);
    }
}

async fn read_parquet(uri: &str) {
    let io_config = daft_io::IOConfig::default();
    let get_result = daft_io::single_url_get(uri.into(), &io_config)
        .await
        .unwrap();

    let s = match get_result {
        GetResult::Stream(s, len) => s,
        GetResult::File(_) => todo!("file"),
    }
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    let val = tokio_util::io::StreamReader::new(s);
}
