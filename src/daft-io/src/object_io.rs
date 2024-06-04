use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;

use tokio::sync::OwnedSemaphorePermit;

use crate::local::{collect_file, LocalFile};
use crate::stats::IOStatsRef;

pub struct StreamingRetryParams {
    pub source: Arc<dyn ObjectSource>,
    pub input: String,
    pub range: Option<Range<usize>>,
    pub io_stats: Option<IOStatsRef>,
}

pub enum GetResult {
    File(LocalFile),
    Stream(
        BoxStream<'static, super::Result<Bytes>>,
        Option<usize>,
        Option<OwnedSemaphorePermit>,
        Option<Box<StreamingRetryParams>>,
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
        let mut get_result = self;
        match get_result {
            File(f) => collect_file(f).await,
            Stream(stream, size, permit, retry_params) => {
                let tries = 3;
                let mut result = collect_bytes(stream, size).await;
                drop(permit); // drop permit to ensure quota
                for _ in 0..tries {
                    match result {
                        Err(super::Error::SocketError { .. })
                        | Err(super::Error::UnableToReadBytes { .. })
                            if let Some(rp) = &retry_params =>
                        {
                            log::warn!(
                                "Received Socket Error, Attempting retry.\nDetails\n {}",
                                result.err().unwrap()
                            );
                            get_result = rp
                                .source
                                .get(&rp.input, rp.range.clone(), rp.io_stats.clone())
                                .await?;
                            if let GetResult::Stream(stream, size, ..) = get_result {
                                result = collect_bytes(stream, size).await;
                            } else {
                                unreachable!("Retrying a stream should always be a stream");
                            }
                        }
                        _ => break,
                    }
                }
                result
            }
        }
    }

    pub fn with_retry(self, params: StreamingRetryParams) -> Self {
        match self {
            GetResult::File(..) => self,
            GetResult::Stream(s, size, permit, _) => {
                GetResult::Stream(s, size, permit, Some(Box::new(params)))
            }
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
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult>;

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize>;

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>>;

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult>;

    async fn iter_dir(
        &self,
        uri: &str,
        posix: bool,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let s = stream! {
            let lsr = self.ls(&uri, posix, None, page_size, io_stats.clone()).await?;
            for fm in lsr.files {
                yield Ok(fm);
            }

            let mut continuation_token = lsr.continuation_token.clone();
            while continuation_token.is_some() {
                let lsr = self.ls(&uri, posix, continuation_token.as_deref(), page_size, io_stats.clone()).await?;
                continuation_token.clone_from(&lsr.continuation_token);
                for fm in lsr.files {
                    yield Ok(fm);
                }
            }
        };
        Ok(s.boxed())
    }
}
