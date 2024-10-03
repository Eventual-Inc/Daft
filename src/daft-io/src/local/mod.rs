#[cfg(test)]
mod tests;

use std::{
    io::{SeekFrom, Write},
    ops::Range,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{
    object_io::{GetResult, ObjectSource},
    Result,
};
use crate::{
    object_io::{self, FileMetadata, LSResult},
    stats::IOStatsRef,
    FileFormat,
};

/// NOTE: We hardcode this even for Windows
///
/// For the most part, Windows machines work quite well with POSIX-style paths
/// as long as there is no "mix" of "\" and "/".
const PATH_SEGMENT_DELIMITER: &str = "/";

pub(crate) struct LocalSource {}

#[derive(Debug, Snafu)]
enum Error {
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
    UnableToSeek {
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

    #[snafu(display("Unexpected symlink when processing directory {}: {}", path, source))]
    UnexpectedSymlink { path: String, source: DaftError },

    #[snafu(display("Unable to convert URL \"{}\" to local file path", path))]
    InvalidFilePath { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::UnableToOpenFile { path, source }
            | Error::UnableToFetchDirectoryEntries { path, source } => {
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
            Error::UnableToFetchFileMetadata { path, source } => {
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
            Error::UnableToReadBytes { path, source } => Self::UnableToReadBytes { path, source },
            Error::UnableToWriteToFile { path, source }
            | Error::UnableToOpenFileForWriting { path, source } => {
                Self::UnableToWriteToFile { path, source }
            }
            _ => Self::Generic {
                store: super::SourceType::File,
                source: error.into(),
            },
        }
    }
}

impl LocalSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        Ok(Self {}.into())
    }
}

pub struct LocalFile {
    pub path: PathBuf,
    pub range: Option<Range<usize>>,
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        const LOCAL_PROTOCOL: &str = "file://";
        if let Some(uri) = uri.strip_prefix(LOCAL_PROTOCOL) {
            Ok(GetResult::File(LocalFile {
                path: uri.into(),
                range,
            }))
        } else {
            Err(Error::InvalidFilePath { path: uri.into() }.into())
        }
    }

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        const LOCAL_PROTOCOL: &str = "file://";
        if let Some(stripped_uri) = uri.strip_prefix(LOCAL_PROTOCOL) {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true) // truncate file if it already exists...
                .write(true)
                .open(stripped_uri)
                .with_context(|_| UnableToOpenFileForWritingSnafu { path: uri })?;
            Ok(file
                .write_all(&data)
                .with_context(|_| UnableToWriteToFileSnafu { path: uri })?)
        } else {
            Err(Error::InvalidFilePath { path: uri.into() }.into())
        }
    }

    async fn get_size(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        const LOCAL_PROTOCOL: &str = "file://";
        let Some(uri) = uri.strip_prefix(LOCAL_PROTOCOL) else {
            return Err(Error::InvalidFilePath { path: uri.into() }.into());
        };
        let meta = tokio::fs::metadata(uri)
            .await
            .context(UnableToFetchFileMetadataSnafu {
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

        // If on Windows, the delimiter provided may be "\" which is treated as an escape character by `glob`
        // We sanitize our filepaths here but note that on-return we will be received POSIX-style paths as well
        #[cfg(target_env = "msvc")]
        {
            let glob_path = glob_path.replace("\\", "/");
            return glob(
                self,
                glob_path.as_str(),
                fanout_limit,
                page_size,
                limit,
                io_stats,
            )
            .await;
        }

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
        posix: bool,
        _page_size: Option<i32>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        if !posix {
            unimplemented!("Prefix-listing is not implemented for local.");
        }

        const LOCAL_PROTOCOL: &str = "file://";
        let uri = if uri.is_empty() {
            std::borrow::Cow::Owned(
                std::env::current_dir()
                    .with_context(|_| UnableToFetchDirectoryEntriesSnafu { path: uri })?
                    .to_string_lossy()
                    .to_string(),
            )
        } else if let Some(uri) = uri.strip_prefix(LOCAL_PROTOCOL) {
            std::borrow::Cow::Borrowed(uri)
        } else {
            return Err(Error::InvalidFilePath { path: uri.into() }.into());
        };

        let meta = tokio::fs::metadata(uri.as_ref()).await.with_context(|_| {
            UnableToFetchFileMetadataSnafu {
                path: uri.to_string(),
            }
        })?;
        if meta.file_type().is_file() {
            // Provided uri points to a file, so only return that file.
            return Ok(futures::stream::iter([Ok(FileMetadata {
                filepath: format!("{}{}", LOCAL_PROTOCOL, uri),
                size: Some(meta.len()),
                filetype: object_io::FileType::File,
            })])
            .boxed());
        }
        let dir_entries = tokio::fs::read_dir(uri.as_ref()).await.with_context(|_| {
            UnableToFetchDirectoryEntriesSnafu {
                path: uri.to_string(),
            }
        })?;
        let dir_stream = tokio_stream::wrappers::ReadDirStream::new(dir_entries);
        let uri = Arc::new(uri.to_string());
        let file_meta_stream = dir_stream.then(move |entry| {
            let uri = uri.clone();
            async move {
                let entry = entry.with_context(|_| UnableToFetchDirectoryEntriesSnafu {
                    path: uri.to_string(),
                })?;

                // NOTE: `entry` returned by ReadDirStream can potentially mix posix-delimiters ("/") and windows-delimiter ("\")
                // on Windows machines if we naively use `entry.path()`. Manually concatting the entries to the uri is safer.
                let path = format!(
                    "{}{PATH_SEGMENT_DELIMITER}{}",
                    uri.trim_end_matches(PATH_SEGMENT_DELIMITER),
                    entry.file_name().to_string_lossy()
                );

                let meta = tokio::fs::metadata(entry.path()).await.with_context(|_| {
                    UnableToFetchFileMetadataSnafu {
                        path: entry.path().to_string_lossy().to_string(),
                    }
                })?;
                Ok(FileMetadata {
                    filepath: format!(
                        "{}{}{}",
                        LOCAL_PROTOCOL,
                        path,
                        if meta.is_dir() {
                            PATH_SEGMENT_DELIMITER
                        } else {
                            ""
                        }
                    ),
                    size: Some(meta.len()),
                    filetype: meta.file_type().try_into().with_context(|_| {
                        UnexpectedSymlinkSnafu {
                            path: entry.path().to_string_lossy().to_string(),
                        }
                    })?,
                })
            }
        });
        Ok(file_meta_stream.boxed())
    }
}

pub(crate) async fn collect_file(local_file: LocalFile) -> Result<Bytes> {
    let path = &local_file.path;
    let mut file = tokio::fs::File::open(path)
        .await
        .context(UnableToOpenFileSnafu {
            path: path.to_string_lossy(),
        })?;

    let mut buf = vec![];

    match local_file.range {
        None => {
            let _ = file
                .read_to_end(&mut buf)
                .await
                .context(UnableToReadBytesSnafu {
                    path: path.to_string_lossy(),
                })?;
        }
        Some(range) => {
            let length = range.end - range.start;
            file.seek(SeekFrom::Start(range.start as u64))
                .await
                .context(UnableToSeekSnafu {
                    path: path.to_string_lossy(),
                })?;
            buf.reserve(length);
            file.take(length as u64)
                .read_to_end(&mut buf)
                .await
                .context(UnableToReadBytesSnafu {
                    path: path.to_string_lossy(),
                })?;
        }
    }
    Ok(Bytes::from(buf))
}
