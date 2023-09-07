use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

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

#[derive(Debug)]
pub enum FileType {
    File,
    Directory,
}
#[derive(Debug)]
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
    async fn ls(
        &self,
        path: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> super::Result<LSResult>;

    async fn iter_dir(&self, uri: &str, delimiter: Option<&str>, _limit: Option<usize>) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let delimiter = delimiter.map(String::from);
        let s = stream! {
            let lsr = self.ls(&uri, delimiter.as_deref(), None).await?;
            let mut continuation_token = lsr.continuation_token.clone();
            for file in lsr.files {
                yield Ok(file);
            }

            while continuation_token.is_some() {
                let lsr = self.ls(&uri, delimiter.as_deref(), continuation_token.as_deref()).await?;
                continuation_token = lsr.continuation_token.clone();
                for file in lsr.files {
                    yield Ok(file);
                }
            }
        };
        Ok(s.boxed())
    }
}

pub(crate) async fn nested(
    source: Arc<dyn ObjectSource>,
    uri: &str,
) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
    let (to_rtn_tx, mut to_rtn_rx) = tokio::sync::mpsc::channel(16 * 1024);
    let sema = Arc::new(tokio::sync::Semaphore::new(64));
    fn add_to_channel(
        source: Arc<dyn ObjectSource>,
        tx: Sender<FileMetadata>,
        dir: String,
        connection_counter: Arc<Semaphore>,
    ) {
        tokio::spawn(async move {
            let _handle = connection_counter.acquire().await.unwrap();
            let mut s = source.iter_dir(&dir, None, None).await.unwrap();
            let tx = &tx;
            while let Some(tr) = s.next().await {
                let tr = tr.unwrap();
                match tr.filetype {
                    FileType::File => tx.send(tr).await.unwrap(),
                    FileType::Directory => add_to_channel(
                        source.clone(),
                        tx.clone(),
                        tr.filepath,
                        connection_counter.clone(),
                    ),
                };
            }
        });
    }

    add_to_channel(source, to_rtn_tx, uri.to_string(), sema);

    let to_rtn_stream = stream! {
        while let Some(v) = to_rtn_rx.recv().await {
            yield Ok(v)
        }
    };

    Ok(to_rtn_stream.boxed())
}
