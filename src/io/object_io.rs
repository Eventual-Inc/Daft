use std::path::PathBuf;
use std::vec;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;

use crate::io::local::collect_file;

pub(crate) enum GetResult {
    File(PathBuf),
    Stream(BoxStream<'static, super::Result<Bytes>>, Option<usize>),
}

async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> super::Result<Bytes>
where
    S: Stream<Item = super::Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()?.unwrap_or_default();
    // Avoid copying if single response
    match stream.next().await.transpose()? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}

impl GetResult {
    pub async fn bytes(self) -> super::Result<Bytes> {
        use GetResult::*;
        match self {
            File(path) => collect_file(path.to_str().unwrap()).await,
            Stream(stream, size) => collect_bytes(stream, size).await,
        }
    }
}

#[async_trait]
pub(crate) trait ObjectSource: Sync + Send {
    async fn get(&self, uri: &str) -> super::Result<GetResult>;
}
