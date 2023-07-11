use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;

use super::object_io::{GetResult, ObjectSource};
use super::Result;
use async_trait::async_trait;
use bytes::Bytes;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
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
        const TO_STRIP: &str = "file://";
        if let Some(p) = uri.strip_prefix(TO_STRIP) {
            let path = std::path::Path::new(p);
            Ok(GetResult::File(LocalFile {
                path: path.to_path_buf(),
                range,
            }))
        } else {
            return Err(Error::InvalidFilePath {
                path: uri.to_string(),
            }
            .into());
        }
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        const TO_STRIP: &str = "file://";
        if let Some(p) = uri.strip_prefix(TO_STRIP) {
            let path = std::path::Path::new(p);
            let file = tokio::fs::File::open(path)
                .await
                .context(UnableToOpenFileSnafu {
                    path: path.to_string_lossy(),
                })?;
            let metadata = file.metadata().await.context(UnableToOpenFileSnafu {
                path: path.to_string_lossy(),
            })?;
            return Ok(metadata.len() as usize);
        } else {
            return Err(Error::InvalidFilePath {
                path: uri.to_string(),
            }
            .into());
        }
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

    use crate::object_io::ObjectSource;
    use crate::Result;
    use crate::{HttpSource, LocalSource};

    #[tokio::test]
    async fn test_full_get_from_local() -> Result<()> {
        let mut file1 = tempfile::NamedTempFile::new().unwrap();
        let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let client = HttpSource::get_client().await?;
        let parquet_file = client.get(parquet_file_path, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);
        file1.write_all(all_bytes).unwrap();
        file1.flush().unwrap();

        let parquet_file_path = format!("file://{}", file1.path().to_str().unwrap());
        let client = LocalSource::get_client().await?;

        let try_all_bytes = client.get(&parquet_file_path, None).await?.bytes().await?;
        assert_eq!(try_all_bytes.len(), all_bytes.len());
        assert_eq!(try_all_bytes.as_ref(), all_bytes);

        let first_bytes = client
            .get_range(&parquet_file_path, 0..10)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

        let first_bytes = client
            .get_range(&parquet_file_path, 10..100)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

        let last_bytes = client
            .get_range(
                &parquet_file_path,
                (all_bytes.len() - 10)..(all_bytes.len() + 10),
            )
            .await?
            .bytes()
            .await?;
        assert_eq!(last_bytes.len(), 10);
        assert_eq!(last_bytes.as_ref(), &all_bytes[(all_bytes.len() - 10)..]);

        let size_from_get_size = client.get_size(parquet_file_path.as_str()).await?;
        assert_eq!(size_from_get_size, all_bytes.len());

        Ok(())
    }
}
