use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;

use tokio::sync::OwnedSemaphorePermit;

use crate::local::{collect_file, LocalFile};

pub enum GetResult {
    File(LocalFile),
    Stream(
        BoxStream<'static, super::Result<Bytes>>,
        Option<usize>,
        Option<OwnedSemaphorePermit>,
    ),
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
            File(f) => collect_file(f).await,
            Stream(stream, size, _permit) => collect_bytes(stream, size).await,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    File,
    Directory,
}

impl TryFrom<std::fs::FileType> for FileType {
    type Error = DaftError;

    fn try_from(value: std::fs::FileType) -> Result<Self, Self::Error> {
        if value.is_dir() {
            Ok(Self::Directory)
        } else if value.is_file() {
            Ok(Self::File)
        } else if value.is_symlink() {
            Err(DaftError::InternalError(format!("Symlinks should never be encountered when constructing FileMetadata, but got: {:?}", value)))
        } else {
            unreachable!(
                "Can only be a directory, file, or symlink, but got: {:?}",
                value
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub filepath: String,
    pub size: Option<u64>,
    pub filetype: FileType,
}
#[derive(Debug)]
pub struct LSResult {
    pub files: Vec<FileMetadata>,
    pub continuation_token: Option<String>,
}

use async_stream::stream;

#[async_trait]
pub(crate) trait ObjectSource: Sync + Send {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult>;
    async fn get_range(&self, uri: &str, range: Range<usize>) -> super::Result<GetResult> {
        self.get(uri, Some(range)).await
    }
    async fn get_size(&self, uri: &str) -> super::Result<usize>;

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>>;

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> super::Result<LSResult>;

    async fn iter_dir(
        &self,
        uri: &str,
        posix: bool,
        page_size: Option<i32>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let s = stream! {
            let lsr = self.ls(&uri, posix, None, page_size).await?;
            for fm in lsr.files {
                yield Ok(fm);
            }

            let mut continuation_token = lsr.continuation_token.clone();
            while continuation_token.is_some() {
                let lsr = self.ls(&uri, posix, continuation_token.as_deref(), page_size).await?;
                continuation_token = lsr.continuation_token.clone();
                for fm in lsr.files {
                    yield Ok(fm);
                }
            }
        };
        Ok(s.boxed())
    }
}
