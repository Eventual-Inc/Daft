use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::{
    StreamExt,
    stream::{BoxStream, Stream},
};
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    FileFormat,
    local::{LocalFile, collect_file},
    stats::IOStatsRef,
};

pub struct StreamingRetryParams {
    source: Arc<dyn ObjectSource>,
    input: String,
    range: Option<GetRange>,
    io_stats: Option<IOStatsRef>,
}

impl StreamingRetryParams {
    pub(crate) fn new(
        source: Arc<dyn ObjectSource>,
        input: String,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> Self {
        Self {
            source,
            input,
            range,
            io_stats,
        }
    }
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

async fn collect_bytes<S>(
    mut stream: S,
    size_hint: Option<usize>,
    _permit: Option<OwnedSemaphorePermit>,
) -> super::Result<Bytes>
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
        use GetResult::{File, Stream};
        let mut get_result = self;
        match get_result {
            File(f) => collect_file(f).await,
            Stream(stream, size, permit, retry_params) => {
                use rand::Rng;
                const NUM_TRIES: u64 = 3;
                const JITTER_MS: u64 = 2_500;
                const MAX_BACKOFF_MS: u64 = 20_000;

                let mut result = collect_bytes(stream, size, permit).await; // drop permit to ensure quota
                for attempt in 1..NUM_TRIES {
                    match result {
                        Err(
                            super::Error::SocketError { .. }
                            | super::Error::UnableToReadBytes { .. }
                            | super::Error::UnableToOpenFile { .. },
                        ) if let Some(rp) = &retry_params => {
                            let jitter = rand::thread_rng()
                                .gen_range(0..((1 << (attempt - 1)) * JITTER_MS))
                                as u64;
                            let jitter = jitter.min(MAX_BACKOFF_MS);

                            log::warn!(
                                "Received Socket Error when streaming bytes! Attempt {attempt} out of {NUM_TRIES} tries. Trying again in {jitter}ms\nDetails\n{}",
                                result.err().unwrap()
                            );
                            tokio::time::sleep(Duration::from_millis(jitter)).await;

                            get_result = rp
                                .source
                                .get(
                                    &rp.input,
                                    rp.range.clone().map(GetRange::from),
                                    rp.io_stats.clone(),
                                )
                                .await?;
                            if let Self::Stream(stream, size, permit, _) = get_result {
                                result = collect_bytes(stream, size, permit).await;
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

    #[must_use]
    pub fn with_retry(self, params: StreamingRetryParams) -> Self {
        match self {
            Self::File(..) => self,
            Self::Stream(s, size, permit, _) => {
                Self::Stream(s, size, permit, Some(Box::new(params)))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            Err(DaftError::InternalError(format!(
                "Symlinks should never be encountered when constructing FileMetadata, but got: {value:?}"
            )))
        } else {
            unreachable!(
                "Can only be a directory, file, or symlink, but got: {:?}",
                value
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

use crate::{multipart::MultipartWriter, range::GetRange};

#[async_trait]
pub trait ObjectSource: Sync + Send {
    /// Check if the source supports range requests.
    /// Most object sources _should_ support range requests.
    /// Many object sources backed by http servers may not support range requests.
    /// So we need to check if the source supports range requests.
    async fn supports_range(&self, uri: &str) -> super::Result<bool>;

    /// Create a multipart writer to upload via multipart upload.
    /// Return None if the source does not support multipart upload.
    async fn create_multipart_writer(
        self: Arc<Self>,
        _uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        Ok(None)
    }

    /// Return the bytes with given range.
    /// Will return [`Error::InvalidRangeRequest`] if range start is greater than range end
    /// or range start is greater than object size.
    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult>;

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<()>;

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize>;

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>>;

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult>;

    /// Delete the object with the given uri.
    /// Return OK if the object is deleted successfully or the object does not exist.
    async fn delete(&self, _uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<()> {
        Err(super::Error::NotImplementedMethod {
            method: "Deletes is not yet supported! Please file an issue.".to_string(),
        })
    }

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
                // Note: There might some race conditions here that the list response is empty
                // even though the continuation token of previous response is not empty, so skip NotFound error here.
                // TODO(desmond): Ideally we should patch how `ls` produces NotFound errors. See issue #4982
                let lsr_result = self.ls(&uri, posix, continuation_token.as_deref(), page_size, io_stats.clone()).await;
                match lsr_result {
                    Ok(lsr) => {
                        continuation_token.clone_from(&lsr.continuation_token);
                        for fm in lsr.files {
                            yield Ok(fm);
                        }
                    },
                    Err(err) => {
                        if matches!(err, super::Error::NotFound { .. }) {
                            continuation_token = None;
                        } else {
                            yield Err(err);
                        }
                    }
                }
            }
        };
        Ok(s.boxed())
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
