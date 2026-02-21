use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use opendal::{EntryMode, Operator, Scheme};
use snafu::ResultExt;

use crate::{
    FileFormat, GetRange,
    multipart::MultipartWriter,
    object_io::{FileMetadata, FileType, GetResult, LSResult, ObjectSource},
    object_store_glob,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

pub(crate) struct OpenDALSource {
    operator: Operator,
    scheme: String,
}

impl OpenDALSource {
    /// List the OpenDAL service schemes that are compiled into this build.
    fn available_schemes() -> &'static [&'static str] {
        &["oss", "cos", "obs", "memory", "fs", "github"]
    }

    pub async fn get_client(
        scheme: &str,
        config: &BTreeMap<String, String>,
    ) -> super::Result<Arc<dyn ObjectSource>> {
        let parsed_scheme: Scheme =
            scheme
                .parse()
                .map_err(|e: opendal::Error| super::Error::UnableToCreateClient {
                    store: super::SourceType::OpenDAL {
                        scheme: scheme.to_string(),
                    },
                    source: format!(
                        "Unknown scheme '{}'. Available OpenDAL schemes: [{}]. Error: {}",
                        scheme,
                        Self::available_schemes().join(", "),
                        e
                    )
                    .into(),
                })?;

        let operator =
            Operator::via_iter(parsed_scheme, config.clone()).map_err(|e: opendal::Error| {
                super::Error::UnableToCreateClient {
                    store: super::SourceType::OpenDAL {
                        scheme: scheme.to_string(),
                    },
                    source: format!(
                        "Failed to create OpenDAL operator for '{}'. \
                         You may need to configure it via IOConfig(opendal_backends={{\"{}\": {{...}}}}). \
                         Error: {}",
                        scheme, scheme, e
                    )
                    .into(),
                }
            })?;

        Ok(Arc::new(Self {
            operator,
            scheme: scheme.to_string(),
        }))
    }
}

/// Extract the path component from a URL like `oss://bucket/path/to/file`.
/// OpenDAL operators are already configured with the root/bucket, so we only
/// need the path portion.
fn url_to_opendal_path(uri: &str) -> super::Result<String> {
    let parsed = url::Url::parse(uri).context(super::InvalidUrlSnafu { path: uri })?;
    // url::Url::path() returns the path component, e.g. "/path/to/file"
    // We strip the leading "/" since OpenDAL paths are relative to the operator root.
    let path = parsed.path();
    let path = path.strip_prefix('/').unwrap_or(path);
    Ok(path.to_string())
}

pub struct OpenDALMultipartWriter {
    writer: opendal::Writer,
    scheme: String,
}

#[async_trait]
impl MultipartWriter for OpenDALMultipartWriter {
    fn part_size(&self) -> usize {
        5 * 1024 * 1024 // 5MB
    }

    async fn put_part(&mut self, data: Bytes) -> super::Result<()> {
        self.writer
            .write(data)
            .await
            .map_err(|e| super::Error::Generic {
                store: super::SourceType::OpenDAL {
                    scheme: self.scheme.clone(),
                },
                source: e.into(),
            })
    }

    async fn complete(&mut self) -> super::Result<()> {
        self.writer
            .close()
            .await
            .map(|_| ())
            .map_err(|e| super::Error::Generic {
                store: super::SourceType::OpenDAL {
                    scheme: self.scheme.clone(),
                },
                source: e.into(),
            })
    }
}

fn opendal_err_to_daft_err(e: opendal::Error, uri: &str, scheme: &str) -> super::Error {
    let source_type = super::SourceType::OpenDAL {
        scheme: scheme.to_string(),
    };
    match e.kind() {
        opendal::ErrorKind::NotFound => super::Error::NotFound {
            path: uri.to_string(),
            source: e.into(),
        },
        opendal::ErrorKind::PermissionDenied => super::Error::Unauthorized {
            store: source_type,
            path: uri.to_string(),
            source: e.into(),
        },
        opendal::ErrorKind::RateLimited => super::Error::Throttled {
            path: uri.to_string(),
            source: e.into(),
        },
        _ => super::Error::Generic {
            store: source_type,
            source: e.into(),
        },
    }
}

#[async_trait]
impl ObjectSource for OpenDALSource {
    async fn supports_range(&self, _uri: &str) -> super::Result<bool> {
        Ok(true)
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        let path = url_to_opendal_path(uri)?;
        let writer = self
            .operator
            .writer(&path)
            .await
            .map_err(|e| opendal_err_to_daft_err(e, uri, &self.scheme))?;
        Ok(Some(Box::new(OpenDALMultipartWriter {
            writer,
            scheme: self.scheme.clone(),
        })))
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let path = url_to_opendal_path(uri)?;

        let reader = self
            .operator
            .reader(&path)
            .await
            .map_err(|e| opendal_err_to_daft_err(e, uri, &self.scheme))?;

        let scheme = self.scheme.clone();
        let uri_owned = uri.to_string();
        let (byte_stream, size) = match range {
            Some(GetRange::Bounded(r)) => {
                let size = Some(r.end - r.start);
                let stream = reader
                    .into_bytes_stream(r.start as u64..r.end as u64)
                    .await
                    .map_err(|e| opendal_err_to_daft_err(e, &uri_owned, &scheme))?;
                (stream, size)
            }
            Some(GetRange::Offset(offset)) => {
                let stream = reader
                    .into_bytes_stream(offset as u64..)
                    .await
                    .map_err(|e| opendal_err_to_daft_err(e, &uri_owned, &scheme))?;
                (stream, None)
            }
            Some(GetRange::Suffix(n)) => {
                let meta = self
                    .operator
                    .stat(&path)
                    .await
                    .map_err(|e| opendal_err_to_daft_err(e, &uri_owned, &scheme))?;
                let file_size = meta.content_length();
                let start = file_size.saturating_sub(n as u64);
                let size = Some((file_size - start) as usize);
                let stream = reader
                    .into_bytes_stream(start..file_size)
                    .await
                    .map_err(|e| opendal_err_to_daft_err(e, &uri_owned, &scheme))?;
                (stream, size)
            }
            None => {
                let stream = reader
                    .into_bytes_stream(..)
                    .await
                    .map_err(|e| opendal_err_to_daft_err(e, &uri_owned, &scheme))?;
                (stream, None)
            }
        };

        use futures::StreamExt;
        let mapped_stream = byte_stream.map(move |result| {
            result.map_err(|e| super::Error::Generic {
                store: super::SourceType::OpenDAL {
                    scheme: scheme.clone(),
                },
                source: e.into(),
            })
        });
        let owned_stream = Box::pin(mapped_stream);
        let stream_with_stats = io_stats_on_bytestream(owned_stream, io_stats);
        Ok(GetResult::Stream(stream_with_stats, size, None, None))
    }

    async fn put(
        &self,
        uri: &str,
        data: Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        let path = url_to_opendal_path(uri)?;
        self.operator
            .write(&path, data)
            .await
            .map(|_| ())
            .map_err(|e| opendal_err_to_daft_err(e, uri, &self.scheme))
    }

    async fn get_size(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let path = url_to_opendal_path(uri)?;
        let meta = self
            .operator
            .stat(&path)
            .await
            .map_err(|e| opendal_err_to_daft_err(e, uri, &self.scheme))?;
        Ok(meta.content_length() as usize)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        _file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        object_store_glob::glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        _page_size: Option<i32>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let opendal_path = url_to_opendal_path(path)?;

        // Ensure path ends with "/" for directory listing
        let dir_path = if opendal_path.is_empty() || opendal_path.ends_with('/') {
            opendal_path
        } else {
            format!("{}/", opendal_path)
        };

        // OpenDAL doesn't natively support continuation tokens, so we list everything.
        // If there is a continuation token, we return empty (already listed).
        if continuation_token.is_some() {
            return Ok(LSResult {
                files: vec![],
                continuation_token: None,
            });
        }

        let entries = if posix {
            // Non-recursive listing (like ls)
            self.operator
                .list(&dir_path)
                .await
                .map_err(|e| opendal_err_to_daft_err(e, path, &self.scheme))?
        } else {
            // Recursive listing
            self.operator
                .list_with(&dir_path)
                .recursive(true)
                .await
                .map_err(|e| opendal_err_to_daft_err(e, path, &self.scheme))?
        };

        // Reconstruct the URL prefix for file paths
        let parsed = url::Url::parse(path).context(super::InvalidUrlSnafu { path })?;
        let base_url = if let Some(host) = parsed.host_str() {
            format!("{}://{}", parsed.scheme(), host)
        } else {
            format!("{}://", parsed.scheme())
        };

        let files = entries
            .into_iter()
            .filter_map(|entry| {
                let entry_path = entry.path();
                // Skip the directory itself
                if entry_path == dir_path || entry_path.is_empty() {
                    return None;
                }
                let filepath = format!("{}/{}", base_url, entry_path);
                let filetype = match entry.metadata().mode() {
                    EntryMode::DIR => FileType::Directory,
                    _ => FileType::File,
                };
                let size = if filetype == FileType::File {
                    Some(entry.metadata().content_length())
                } else {
                    None
                };
                Some(FileMetadata {
                    filepath,
                    size,
                    filetype,
                })
            })
            .collect();

        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }

    async fn delete(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let path = url_to_opendal_path(uri)?;
        self.operator
            .delete(&path)
            .await
            .map_err(|e| opendal_err_to_daft_err(e, uri, &self.scheme))
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_opendal_memory_put_get_roundtrip() {
        let config: BTreeMap<String, String> = BTreeMap::new();
        let source = OpenDALSource::get_client("memory", &config)
            .await
            .expect("Failed to create memory client");

        // Put data
        let data = Bytes::from("hello opendal");
        source
            .put("memory://test/hello.txt", data.clone(), None)
            .await
            .expect("put failed");

        // Get data
        let result = source
            .get("memory://test/hello.txt", None, None)
            .await
            .expect("get failed");
        let bytes = result.bytes().await.expect("bytes failed");
        assert_eq!(bytes, data);
    }

    #[tokio::test]
    async fn test_opendal_memory_get_size() {
        let config: BTreeMap<String, String> = BTreeMap::new();
        let source = OpenDALSource::get_client("memory", &config)
            .await
            .expect("Failed to create memory client");

        let data = Bytes::from("hello opendal");
        source
            .put("memory://test/size.txt", data.clone(), None)
            .await
            .expect("put failed");

        let size = source
            .get_size("memory://test/size.txt", None)
            .await
            .expect("get_size failed");
        assert_eq!(size, 13);
    }

    #[tokio::test]
    async fn test_opendal_memory_delete() {
        let config: BTreeMap<String, String> = BTreeMap::new();
        let source = OpenDALSource::get_client("memory", &config)
            .await
            .expect("Failed to create memory client");

        let data = Bytes::from("to be deleted");
        source
            .put("memory://test/delete.txt", data, None)
            .await
            .expect("put failed");

        source
            .delete("memory://test/delete.txt", None)
            .await
            .expect("delete failed");

        let result = source.get_size("memory://test/delete.txt", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_opendal_memory_get_range() {
        let config: BTreeMap<String, String> = BTreeMap::new();
        let source = OpenDALSource::get_client("memory", &config)
            .await
            .expect("Failed to create memory client");

        let data = Bytes::from("hello opendal world");
        source
            .put("memory://test/range.txt", data, None)
            .await
            .expect("put failed");

        // Test bounded range
        let result = source
            .get(
                "memory://test/range.txt",
                Some(GetRange::Bounded(0..5)),
                None,
            )
            .await
            .expect("get with range failed");
        let bytes = result.bytes().await.expect("bytes failed");
        assert_eq!(bytes, Bytes::from("hello"));

        // Test offset range
        let result = source
            .get("memory://test/range.txt", Some(GetRange::Offset(6)), None)
            .await
            .expect("get with offset failed");
        let bytes = result.bytes().await.expect("bytes failed");
        assert_eq!(bytes, Bytes::from("opendal world"));
    }

    #[tokio::test]
    async fn test_opendal_memory_ls() {
        let config: BTreeMap<String, String> = BTreeMap::new();
        let source = OpenDALSource::get_client("memory", &config)
            .await
            .expect("Failed to create memory client");

        source
            .put("memory://test/a.txt", Bytes::from("a"), None)
            .await
            .unwrap();
        source
            .put("memory://test/b.txt", Bytes::from("b"), None)
            .await
            .unwrap();

        let result = source
            .ls("memory://test/", true, None, None, None)
            .await
            .expect("ls failed");
        assert!(result.files.len() >= 2);
    }

    #[test]
    fn test_url_to_opendal_path() {
        assert_eq!(
            url_to_opendal_path("oss://my-bucket/path/to/file.parquet").unwrap(),
            "path/to/file.parquet"
        );
        assert_eq!(url_to_opendal_path("cos://bucket/dir/").unwrap(), "dir/");
        assert_eq!(
            url_to_opendal_path("memory://test/hello.txt").unwrap(),
            "hello.txt"
        );
    }
}
