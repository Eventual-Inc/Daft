use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;

use crate::object_io::{self, FileMetadata, LSResult};

use super::object_io::{GetResult, ObjectSource};
use super::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::StreamExt;
use url::ParseError;
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

    #[snafu(display("Unable to parse URL \"{}\"", url.to_string_lossy()))]
    InvalidUrl { url: PathBuf, source: ParseError },

    #[snafu(display("Unable to convert URL \"{}\" to local file path", path))]
    InvalidFilePath { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source } => {
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
    path: PathBuf,
    range: Option<Range<usize>>,
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        const LOCAL_PROTOCOL: &str = "file://";
        let path = uri.strip_prefix(LOCAL_PROTOCOL).unwrap_or(uri);
        Ok(GetResult::File(LocalFile {
            path: path.into(),
            range,
        }))
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        const LOCAL_PROTOCOL: &str = "file://";
        let path = uri.strip_prefix(LOCAL_PROTOCOL).unwrap_or(uri);
        let meta = tokio::fs::metadata(path)
            .await
            .context(UnableToFetchFileMetadataSnafu {
                path: path.to_string(),
            })?;
        Ok(meta.len() as usize)
    }

    async fn ls(
        &self,
        path: &str,
        _delimiter: Option<&str>,
        _continuation_token: Option<&str>,
    ) -> super::Result<LSResult> {
        const LOCAL_PROTOCOL: &str = "file://";
        let path = path.strip_prefix(LOCAL_PROTOCOL).unwrap_or(path);
        let meta = tokio::fs::metadata(path)
            .await
            .context(UnableToFetchFileMetadataSnafu {
                path: path.to_string(),
            })?;
        if meta.file_type().is_file() {
            // Provided path points to a file, so only return that file.
            return Ok(LSResult {
                files: vec![FileMetadata {
                    filepath: path.into(),
                    size: Some(meta.len()),
                    filetype: object_io::FileType::File,
                }],
                continuation_token: None,
            });
        }
        // NOTE(Clark): read_dir follows symbolic links, so no special handling is needed there.
        let dir_entries =
            tokio::fs::read_dir(path)
                .await
                .context(UnableToFetchDirectoryEntriesSnafu {
                    path: path.to_string(),
                })?;
        let mut dir_stream = tokio_stream::wrappers::ReadDirStream::new(dir_entries);
        let size = dir_stream.size_hint().1.unwrap_or(0);
        let mut files = Vec::with_capacity(size);
        while let Some(entry) =
            dir_stream
                .next()
                .await
                .transpose()
                .context(UnableToFetchDirectoryEntriesSnafu {
                    path: path.to_string(),
                })?
        {
            let meta = tokio::fs::metadata(entry.path()).await.context(
                UnableToFetchDirectoryEntriesSnafu {
                    path: entry.path().to_string_lossy(),
                },
            )?;
            files.push(FileMetadata {
                filepath: entry.path().to_string_lossy().to_string(),
                size: Some(meta.len()),
                filetype: meta.file_type().into(),
            })
        }
        Ok(LSResult {
            files,
            continuation_token: None,
        })
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

    async fn _test_local_full_ls(protocol_prefix: bool) -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let mut file1 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file1).await?;
        let mut file2 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file2).await?;
        let mut file3 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
        write_remote_parquet_to_local_file(&mut file3).await?;
        let dir_path = if protocol_prefix {
            format!("file://{}", dir.path().to_string_lossy())
        } else {
            dir.path().to_string_lossy().into()
        };
        let client = LocalSource::get_client().await?;

        let ls_result = client.ls(dir_path.as_ref(), None, None).await?;
        let mut files = ls_result.files.clone();
        // Ensure stable sort ordering of file paths before comparing with expected payload.
        files.sort_by(|a, b| a.filepath.cmp(&b.filepath));
        let mut expected = vec![
            FileMetadata {
                filepath: file1.path().to_string_lossy().to_string(),
                size: Some(file1.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
            FileMetadata {
                filepath: file2.path().to_string_lossy().to_string(),
                size: Some(file2.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
            FileMetadata {
                filepath: file3.path().to_string_lossy().to_string(),
                size: Some(file3.as_file().metadata().unwrap().len()),
                filetype: FileType::File,
            },
        ];
        expected.sort_by(|a, b| a.filepath.cmp(&b.filepath));
        assert_eq!(files, expected);
        assert_eq!(ls_result.continuation_token, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_local_full_ls() -> Result<()> {
        _test_local_full_ls(false).await
    }

    #[tokio::test]
    async fn test_local_full_ls_protocol_prefix() -> Result<()> {
        _test_local_full_ls(true).await
    }
}
