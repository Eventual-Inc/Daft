use crate::error::DaftResult;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;

pub enum GetResult {
    File,
    Stream(BoxStream<'static, DaftResult<Bytes>>),
}

pub async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> DaftResult<Bytes>
where
    S: Stream<Item = DaftResult<Bytes>> + Send + Unpin,
{
    // Taken from https://github.com/apache/arrow-rs/blob/ab56693985826bb8caea30558b8c25db286a5e37/object_store/src/util.rs#LL49C1-L71C2
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
    pub async fn bytes(self) -> DaftResult<Bytes> {
        use GetResult::*;
        match self {
            File => todo!("Impl local fs"),
            Stream(s) => collect_bytes(s, None).await,
        }
    }
}

#[async_trait]
pub trait ObjectSource {
    async fn get(&self, uri: String) -> DaftResult<GetResult>;
}
