use std::{io::Write, ops::Range, sync::Arc};

use async_trait::async_trait;
use futures::{
    stream::{self, BoxStream},
    AsyncReadExt, AsyncSeekExt, StreamExt, TryStreamExt,
};
use hdrs::{Client, ClientBuilder};
use snafu::{IntoError, OptionExt, ResultExt, Snafu};
use url::Url;

use super::{
    object_io::{GetResult, ObjectSource},
    Result,
};
use crate::{
    object_io::{self, FileMetadata, LSResult},
    stats::IOStatsRef,
    FileFormat,
};

pub(crate) struct HDFSSource {}

const HDFS_SCHEME_PREFIX: &str = "hdfs://";

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to connect to hdfs namenode {}: {}", path, source))]
    UnableToConnect {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to open file {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write to file {}: {}", path, source))]
    UnableToWriteToFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to open file for writing {}: {}", path, source))]
    UnableToOpenFileForWriting {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to seek in file {}: {}", path, source))]
    UnableToSeekReader {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to fetch file metadata for file {}: {}", path, source))]
    UnableToFetchFileMetadata {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to get entries for directory {}: {}", path, source))]
    UnableToFetchDirectoryEntries {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to convert URL \"{}\" to local file path", path))]
    InvalidFilePath { path: String },
}

fn get_fs_for(uri: &str) -> Result<Arc<Client>> {
    let name_node = get_namenode_url(uri)?;
    let client = ClientBuilder::new(&name_node)
        .connect()
        .context(UnableToConnectSnafu { path: uri })?;
    Ok(Arc::new(client))
}

fn get_namenode_url(uri: &str) -> Result<String> {
    let parsed = match Url::parse(uri) {
        Ok(parsed) => Ok(parsed),
        Err(_) => Err(Error::InvalidFilePath { path: uri.into() }),
    }?;
    let host = parsed
        .host_str()
        .context(InvalidFilePathSnafu { path: uri })?;
    let port = parsed.port();
    let name_node = match port {
        None => host.to_string(),
        Some(p) => format!("{}:{}", host, p),
    };
    Ok(name_node)
}

fn get_path_for(uri: &str) -> Result<String> {
    match Url::parse(uri) {
        Ok(parsed) => Ok(parsed.path().into()),
        Err(_) => Err(Error::InvalidFilePath { path: uri.into() }.into()),
    }
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToConnect { path, source } => Self::ConnectTimeout {
                path,
                source: source.into(),
            },
            UnableToOpenFile { path, source } | UnableToFetchDirectoryEntries { path, source } => {
                use std::io::ErrorKind::*;
                match source.kind() {
                    NotFound => Self::NotFound {
                        path,
                        source: source.into(),
                    },
                    _ => Self::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            UnableToFetchFileMetadata { path, source } => {
                use std::io::ErrorKind::*;
                match source.kind() {
                    NotFound | IsADirectory => Self::NotFound {
                        path,
                        source: source.into(),
                    },
                    _ => Self::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            UnableToReadBytes { path, source } => Self::UnableToReadBytes { path, source },
            UnableToWriteToFile { path, source } | UnableToOpenFileForWriting { path, source } => {
                Self::UnableToWriteToFile { path, source }
            }
            _ => Self::Generic {
                store: super::SourceType::File,
                source: error.into(),
            },
        }
    }
}

impl HDFSSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        Ok(Self {}.into())
    }
}

#[async_trait]
impl ObjectSource for HDFSSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let fs = get_fs_for(uri)?;
        let path = get_path_for(uri)?;
        let path = &path;
        let len = fs
            .metadata(uri)
            .context(UnableToFetchFileMetadataSnafu { path: uri })?
            .len();
        let mut read_file = fs
            .open_file()
            .read(true)
            .async_open(path)
            .await
            .context(UnableToOpenFileSnafu { path: uri })?;
        let owned_uri = uri.to_string();
        use tokio_util::compat::FuturesAsyncReadCompatExt;
        let stream = if let Some(range) = range {
            read_file
                .seek(std::io::SeekFrom::Start(range.start as u64))
                .await
                .with_context(|_| UnableToSeekReaderSnafu {
                    path: owned_uri.clone(),
                })?;
            let reader = read_file.take(range.len() as u64).compat();
            tokio_util::io::ReaderStream::new(reader)
                .map_err(move |err| {
                    UnableToReadBytesSnafu {
                        path: owned_uri.clone(),
                    }
                    .into_error(err)
                    .into()
                })
                .boxed()
        } else {
            let reader = read_file.compat();
            tokio_util::io::ReaderStream::new(reader)
                .map_err(move |err| {
                    UnableToReadBytesSnafu {
                        path: owned_uri.clone(),
                    }
                    .into_error(err)
                    .into()
                })
                .boxed()
        };
        Ok(GetResult::Stream(stream, Some(len as usize), None, None))
    }

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        let fs = get_fs_for(uri)?;
        let path = get_path_for(uri)?;
        let path = &path;
        let mut file = fs
            .open_file()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .context(UnableToOpenFileForWritingSnafu { path })?;
        Ok(file
            .write_all(&data)
            .context(UnableToWriteToFileSnafu { path: uri })?)
    }

    async fn get_size(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let fs = get_fs_for(uri)?;
        let path = get_path_for(uri)?;
        let path = &path;
        let meta = fs.metadata(path).context(UnableToFetchFileMetadataSnafu {
            path: uri.to_string(),
        })?;

        if meta.is_dir() {
            Err(super::Error::NotAFile {
                path: uri.to_owned(),
            })
        } else {
            Ok(meta.len() as usize)
        }
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        _fanout_limit: Option<usize>,
        _page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        _file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        // Ensure fanout_limit is None because Local ObjectSource does not support prefix listing
        let fanout_limit = None;
        let page_size = None;

        glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        _continuation_token: Option<&str>,
        _page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let s = self.iter_dir(path, posix, None, io_stats).await?;
        let files = s.try_collect::<Vec<_>>().await?;
        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }

    async fn iter_dir(
        &self,
        uri: &str,
        _posix: bool,
        _page_size: Option<i32>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let fs = get_fs_for(uri)?;
        let path = get_path_for(uri)?;
        let name_node = get_namenode_url(uri)?;
        let path = &path;
        let meta = fs
            .metadata(path)
            .context(UnableToFetchFileMetadataSnafu { path })?;
        if meta.is_file() {
            return Ok(futures::stream::iter([Ok(FileMetadata {
                filepath: uri.into(),
                size: Some(meta.len()),
                filetype: object_io::FileType::File,
            })])
            .boxed());
        }
        let dir_entries = fs
            .read_dir(path)
            .context(UnableToFetchDirectoryEntriesSnafu { path })?;
        let dir_entries_iter = dir_entries.into_inner();
        let name_node = Arc::new(name_node);
        let file_meta_stream = stream::unfold(dir_entries_iter, move |mut iter| {
            let name_node_clone = name_node.clone();
            async move {
                match iter.next() {
                    Some(metadata) => {
                        let location = metadata.path().to_string();
                        if metadata.is_dir() {
                            Some((
                                Ok(FileMetadata {
                                    filepath: format!(
                                        "{}{}{}",
                                        HDFS_SCHEME_PREFIX, name_node_clone, location
                                    ),
                                    size: None,
                                    filetype: object_io::FileType::Directory,
                                }),
                                iter,
                            ))
                        } else {
                            Some((
                                Ok(FileMetadata {
                                    filepath: format!(
                                        "{}{}{}",
                                        HDFS_SCHEME_PREFIX, name_node_clone, location
                                    ),
                                    size: Some(metadata.len()),
                                    filetype: object_io::FileType::File,
                                }),
                                iter,
                            ))
                        }
                    }
                    None => None,
                }
            }
        })
        .boxed();
        Ok(file_meta_stream)
    }
}
