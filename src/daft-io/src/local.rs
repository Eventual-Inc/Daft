use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;

use crate::object_io::{self, FileMetadata, LSResult};

use super::object_io::{GetResult, ObjectSource};
use super::Result;
use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use url::ParseError;

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

    #[snafu(display("Unable to parse URL \"{}\"", url.to_string_lossy()))]
    InvalidUrl { url: PathBuf, source: ParseError },

    #[snafu(display("Unable to convert URL \"{}\" to local file path", path))]
    InvalidFilePath { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source }
            | UnableToFetchFileMetadata { path, source }
            | UnableToFetchDirectoryEntries { path, source } => {
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
            UnableToReadBytes { path, source } => super::Error::UnableToReadBytes { path, source },
            InvalidUrl { url, source } => super::Error::InvalidUrl {
                path: url.to_string_lossy().into_owned(),
                source,
            },
            _ => super::Error::Generic {
                store: super::SourceType::File,
                source: error.into(),
            },
        }
    }
}

impl LocalSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        Ok(LocalSource {}.into())
    }
}

pub struct LocalFile {
    pub path: PathBuf,
    pub range: Option<Range<usize>>,
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
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

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        const LOCAL_PROTOCOL: &str = "file://";
        let Some(uri) = uri.strip_prefix(LOCAL_PROTOCOL) else {
            return Err(Error::InvalidFilePath { path: uri.into() }.into());
        };
        let meta = tokio::fs::metadata(uri)
            .await
            .context(UnableToFetchFileMetadataSnafu {
                path: uri.to_string(),
            })?;
        Ok(meta.len() as usize)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        _fanout_limit: Option<usize>,
        _page_size: Option<i32>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        // Ensure fanout_limit is None because Local ObjectSource does not support prefix listing
        let fanout_limit = None;
        let page_size = None;

        // If on Windows, the delimiter provided may be "\" which is treated as an escape character by `glob`
        // We sanitize our filepaths here but note that on-return we will be received POSIX-style paths as well
        #[cfg(target_env = "msvc")]
        {
            let glob_path = glob_path.replace("\\", "/");
            return glob(self, glob_path.as_str(), fanout_limit, page_size).await;
        }

        glob(self, glob_path, fanout_limit, page_size).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        _continuation_token: Option<&str>,
        _page_size: Option<i32>,
    ) -> super::Result<LSResult> {
        let s = self.iter_dir(path, posix, None).await?;
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
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        if !posix {
            unimplemented!("Prefix-listing is not implemented for local.");
        }

        const LOCAL_PROTOCOL: &str = "file://";
        let Some(uri) = uri.strip_prefix(LOCAL_PROTOCOL) else {
            return Err(Error::InvalidFilePath { path: uri.into() }.into());
        };
        let meta =
            tokio::fs::metadata(uri)
                .await
                .with_context(|_| UnableToFetchFileMetadataSnafu {
                    path: uri.to_string(),
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
        let dir_entries = tokio::fs::read_dir(uri).await.with_context(|_| {
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

#[cfg(test)]

mod tests {
    use std::io::Write;

    use crate::object_io::{FileMetadata, FileType, ObjectSource};
    use crate::Result;
    use crate::{HttpSource, LocalSource};

    async fn write_remote_parquet_to_local_file(
        f: &mut tempfile::NamedTempFile,
    ) -> Result<bytes::Bytes> {
        let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let client = HttpSource::get_client().await?;
        let parquet_file = client.get(parquet_file_path, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);
        f.write_all(all_bytes).unwrap();
        f.flush().unwrap();
        Ok(bytes)
    }

    #[tokio::test]
    async fn test_local_full_get() -> Result<()> {
        let mut file1 = tempfile::NamedTempFile::new().unwrap();
        let bytes = write_remote_parquet_to_local_file(&mut file1).await?;

        let parquet_file_path = format!("file://{}", file1.path().to_str().unwrap());
        let client = LocalSource::get_client().await?;

        let try_all_bytes = client.get(&parquet_file_path, None).await?.bytes().await?;
        assert_eq!(try_all_bytes.len(), bytes.len());
        assert_eq!(try_all_bytes, bytes);

        let first_bytes = client
            .get_range(&parquet_file_path, 0..10)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &bytes[..10]);

        let first_bytes = client
            .get_range(&parquet_file_path, 10..100)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &bytes[10..100]);

        let last_bytes = client
            .get_range(&parquet_file_path, (bytes.len() - 10)..(bytes.len() + 10))
            .await?
            .bytes()
            .await?;
        assert_eq!(last_bytes.len(), 10);
        assert_eq!(last_bytes.as_ref(), &bytes[(bytes.len() - 10)..]);

        let size_from_get_size = client.get_size(parquet_file_path.as_str()).await?;
        assert_eq!(size_from_get_size, bytes.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_full_ls() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let mut file1 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file1).await?;
        let mut file2 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file2).await?;
        let mut file3 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file3).await?;
        let dir_path = format!("file://{}", dir.path().to_string_lossy().replace("\\", "/"));
        let client = LocalSource::get_client().await?;

        let ls_result = client.ls(dir_path.as_ref(), true, None, None).await?;
        let mut files = ls_result.files.clone();
        // Ensure stable sort ordering of file paths before comparing with expected payload.
        files.sort_by(|a, b| a.filepath.cmp(&b.filepath));
        let mut expected = vec![
            FileMetadata {
                filepath: format!(
                    "file://{}/{}",
                    dir.path().to_string_lossy().replace("\\", "/"),
                    file1.path().file_name().unwrap().to_string_lossy(),
                ),
                size: Some(file1.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
            FileMetadata {
                filepath: format!(
                    "file://{}/{}",
                    dir.path().to_string_lossy().replace("\\", "/"),
                    file2.path().file_name().unwrap().to_string_lossy(),
                ),
                size: Some(file2.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
            FileMetadata {
                filepath: format!(
                    "file://{}/{}",
                    dir.path().to_string_lossy().replace("\\", "/"),
                    file3.path().file_name().unwrap().to_string_lossy(),
                ),
                size: Some(file3.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
        ];
        expected.sort_by(|a, b| a.filepath.cmp(&b.filepath));
        assert_eq!(files, expected);
        assert_eq!(ls_result.continuation_token, None);

        Ok(())
    }
}
