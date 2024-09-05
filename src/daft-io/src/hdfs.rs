use std::io::Write;
use std::ops::Range;

use crate::object_io::{self, FileMetadata, LSResult};
use crate::stats::IOStatsRef;
use crate::FileFormat;

use super::object_io::{GetResult, ObjectSource};
use super::Result;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use futures::{AsyncReadExt, AsyncSeekExt, StreamExt};
use snafu::{IntoError, OptionExt, ResultExt, Snafu};
use std::sync::Arc;

use hdrs::{Client, ClientBuilder};
use url::Url;

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

fn _get_fs_for(uri: &str) -> Result<Arc<Client>> {
    let name_node = _get_namenode_url(uri)?;
    let client = ClientBuilder::new(&name_node)
        .connect()
        .context(UnableToConnectSnafu { path: uri })?;
    Ok(Arc::new(client))
}

fn _get_namenode_url(uri: &str) -> Result<String> {
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

fn _get_path_for(uri: &str) -> Result<String> {
    match Url::parse(uri) {
        Ok(parsed) => Ok(parsed.path().into()),
        Err(_) => Err(Error::InvalidFilePath { path: uri.into() }.into()),
    }
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToConnect { path, source } => super::Error::ConnectTimeout {
                path,
                source: source.into(),
            },
            UnableToOpenFile { path, source } | UnableToFetchDirectoryEntries { path, source } => {
                use std::io::ErrorKind::*;
                match source.kind() {
                    NotFound => super::Error::NotFound {
                        path,
                        source: source.into(),
                    },
                    _ => super::Error::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            UnableToFetchFileMetadata { path, source } => {
                use std::io::ErrorKind::*;
                match source.kind() {
                    NotFound | IsADirectory => super::Error::NotFound {
                        path,
                        source: source.into(),
                    },
                    _ => super::Error::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            UnableToReadBytes { path, source } => super::Error::UnableToReadBytes { path, source },
            UnableToWriteToFile { path, source } | UnableToOpenFileForWriting { path, source } => {
                super::Error::UnableToWriteToFile { path, source }
            }
            _ => super::Error::Generic {
                store: super::SourceType::File,
                source: error.into(),
            },
        }
    }
}

impl HDFSSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        Ok(HDFSSource {}.into())
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
        let fs = _get_fs_for(uri)?;
        let path = _get_path_for(uri)?;
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
        let fs = _get_fs_for(uri)?;
        let path = _get_path_for(uri)?;
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
        let fs = _get_fs_for(uri)?;
        let path = _get_path_for(uri)?;
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
        let fs = _get_fs_for(uri)?;
        let path = _get_path_for(uri)?;
        let name_node = _get_namenode_url(uri)?;
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

#[cfg(test)]

mod tests {
    use std::default;

    use crate::object_io::ObjectSource;
    use crate::Result;
    use crate::{HDFSSource, HttpSource};

    async fn write_remote_parquet_to_hdfs_file(path: &str) -> Result<bytes::Bytes> {
        let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let client = HttpSource::get_client(&default::Default::default()).await?;
        let parquet_file = client.get(parquet_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);
        let hdfs_fs = HDFSSource::get_client().await?;
        let uri = format!("hdfs://localhost:9000{}", path);
        let bytes_clone = bytes.clone();
        hdfs_fs.put(&uri, bytes_clone, None).await?;
        Ok(bytes)
    }

    #[tokio::test]
    async fn test_hdfs_full_get() -> Result<()> {
        use crate::hdfs::*;
        let path = "/data_file_for_full_get_test.parquet";

        let _bytes = write_remote_parquet_to_hdfs_file(path).await?;
        let full_uri = format!("hdfs://localhost:9000{}", path);
        let parquet_file_path = &full_uri;
        let client = HDFSSource::get_client().await?;

        let try_all_bytes = client
            .get(parquet_file_path, None, None)
            .await?
            .bytes()
            .await?;
        assert_eq!(try_all_bytes.len(), _bytes.len());
        assert_eq!(try_all_bytes, _bytes);

        let first_bytes = client
            .get(parquet_file_path, Some(0..10), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &_bytes[..10]);

        let first_bytes = client
            .get(parquet_file_path, Some(10..100), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &_bytes[10..100]);

        let last_bytes = client
            .get(
                parquet_file_path,
                Some((_bytes.len() - 10)..(_bytes.len() + 10)),
                None,
            )
            .await?
            .bytes()
            .await?;
        assert_eq!(last_bytes.len(), 10);
        assert_eq!(last_bytes.as_ref(), &_bytes[(_bytes.len() - 10)..]);

        let size_from_get_size = client.get_size(parquet_file_path, None).await?;
        assert_eq!(size_from_get_size, _bytes.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_hdfs_full_ls() -> Result<()> {
        use crate::hdfs::*;

        let path = "/data_file_for_full_ls_test.parquet";
        let _ = write_remote_parquet_to_hdfs_file(path).await?;

        let full_uri = format!("hdfs://127.0.0.1:9000{}", path);
        let parquet_file_path = &full_uri;
        let client = HDFSSource::get_client().await?;

        let ls_result = client.ls(parquet_file_path, true, None, None, None).await?;
        assert_eq!(ls_result.files.len(), 1);
        assert!(ls_result.files[0].filepath.starts_with(HDFS_SCHEME_PREFIX));
        Ok(())
    }
}
