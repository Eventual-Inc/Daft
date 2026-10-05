use std::{any::Any, collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use opendal::{EntryMode, Operator, layers::TimeoutLayer};
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
    /// Deadline-bounded operator for reads and metadata operations.
    operator: Operator,
    /// Unbounded operator for writes and deletes; see `get_client` for why.
    write_operator: Operator,
    scheme: String,
}

impl OpenDALSource {
    /// List the OpenDAL service schemes that are compiled into this build.
    fn available_schemes() -> Vec<&'static str> {
        #[cfg_attr(not(feature = "hdfs"), allow(unused_mut))]
        let mut schemes = vec![
            "oss", "cos", "obs", "tos", "goosefs", "memory", "fs", "github",
        ];
        #[cfg(feature = "hdfs")]
        schemes.push("hdfs");
        schemes
    }

    pub async fn get_client(
        scheme: &str,
        config: &BTreeMap<String, String>,
    ) -> super::Result<Arc<dyn ObjectSource>> {
        // Register compiled-in OpenDAL services and install the process-wide
        // HTTP transport. OpenDAL 0.58+ splits HTTP into a separate transport
        // that must be installed before cloud services can make requests.
        // Safe to call repeatedly (registry init and transport install are once).
        opendal::install_default();

        let operator =
            Operator::via_iter(scheme, config.clone()).map_err(|e: opendal::Error| {
                super::Error::UnableToCreateClient {
                    store: super::SourceType::OpenDAL {
                        scheme: scheme.to_string(),
                    },
                    source: format!(
                        "Failed to create OpenDAL operator for '{}'. \
                         You may need to configure it via IOConfig(opendal_backends={{\"{}\": {{...}}}}). \
                         Available OpenDAL schemes: [{}]. \
                         Error: {}",
                        scheme, scheme, Self::available_schemes().join(", "), e
                    )
                    .into(),
                }
            })?;

        // OpenDAL's default operator stack carries no deadlines and its shared
        // HTTP client never times out, so a silently dropped connection (e.g.
        // a load balancer reaping idle connections without a RST) would stall
        // reads forever. Enforce OpenDAL's TimeoutLayer defaults on the read
        // path (60s per control operation, 10s between body reads), overridable
        // per backend via the `timeout_ms` / `io_timeout_ms` config keys.
        //
        // Writes and deletes deliberately keep their previous unbounded
        // behavior: io_timeout bounds the *whole* body of a write call, so any
        // finite default would also cap how long a part upload may take and
        // break slow-but-legitimate uploads.
        //
        // NOTE: if a RetryLayer is ever added here, keep it outside the
        // TimeoutLayer; a TimeoutLayer added outside a RetryLayer can drop
        // retry futures mid-state-restore (see TimeoutLayer's
        // cancellation-safety notes in opendal).
        let mut timeouts = TimeoutLayer::default();
        if let Some(timeout) = config_duration_ms(scheme, config, "timeout_ms")? {
            timeouts = timeouts.with_timeout(timeout);
        }
        if let Some(io_timeout) = config_duration_ms(scheme, config, "io_timeout_ms")? {
            timeouts = timeouts.with_io_timeout(io_timeout);
        }
        for key in ["timeout", "io_timeout"] {
            if config.contains_key(key) {
                log::warn!(
                    "OpenDAL backend config key '{key}' is not an OpenDAL service setting and \
                     is ignored; use 'timeout_ms' / 'io_timeout_ms' to configure deadlines"
                );
            }
        }
        let write_operator = operator.clone();
        let operator = operator.layer(timeouts);

        Ok(Arc::new(Self {
            operator,
            write_operator,
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

/// Largest accepted deadline override (24h), as a sanity bound: operations
/// slower than this should be treated as hung rather than waited out.
const MAX_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;

/// Read a deadline override from an OpenDAL backend config map. OpenDAL
/// services ignore config keys they do not know, so daft-specific deadline
/// keys ride along in the same map; malformed values fail client creation
/// rather than silently dropping the user's timeout.
fn config_duration_ms(
    scheme: &str,
    config: &BTreeMap<String, String>,
    key: &str,
) -> super::Result<Option<Duration>> {
    let Some(raw) = config.get(key) else {
        return Ok(None);
    };
    let invalid = || super::Error::UnableToCreateClient {
        store: super::SourceType::OpenDAL {
            scheme: scheme.to_string(),
        },
        source: format!(
            "invalid config value for '{key}': '{raw}' \
             (expected milliseconds between 1 and {MAX_TIMEOUT_MS})"
        )
        .into(),
    };
    let millis = raw.parse::<u64>().map_err(|_| invalid())?;
    if !(1..=MAX_TIMEOUT_MS).contains(&millis) {
        return Err(invalid());
    }
    Ok(Some(Duration::from_millis(millis)))
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
            .write_operator
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
            result.map_err(|e: std::io::Error| super::Error::Generic {
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
        self.write_operator
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
        if meta.is_dir() {
            return Err(super::Error::NotAFile {
                path: uri.to_string(),
            });
        }
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
                not_found_if_empty: false,
            });
        }

        let entries = self
            .operator
            .list_with(&dir_path)
            .recursive(!posix)
            .await
            .map_err(|e| opendal_err_to_daft_err(e, path, &self.scheme))?;

        // Reconstruct the URL prefix for file paths.
        // Use authority (host:port) rather than host_str so schemes like HDFS
        // (hdfs://host:port) generate correct URLs.
        let parsed = url::Url::parse(path).context(super::InvalidUrlSnafu { path })?;
        let base_url = if !parsed.authority().is_empty() {
            format!("{}://{}", parsed.scheme(), parsed.authority())
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
            not_found_if_empty: false,
        })
    }

    async fn delete(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let path = url_to_opendal_path(uri)?;
        self.write_operator
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
    use std::io::{Read, Write};

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

    /// Serve valid HTTP response headers that promise a body, then never send
    /// the body — what a client sees when a connection is silently dropped
    /// mid-response (e.g. an LB reaping idle connections without a RST).
    fn spawn_stalled_http_server() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            while let Ok((mut socket, _)) = listener.accept() {
                std::thread::spawn(move || {
                    // Drain the request; we do not care about its contents.
                    let _ = socket.read(&mut [0u8; 4096]);
                    let _ = socket.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: close\r\n\r\n",
                    );
                    // Hold the socket open until the client gives up: the
                    // promised body never arrives.
                    let _ = socket.read(&mut [0u8; 4096]);
                });
            }
        });
        format!("http://{addr}")
    }

    /// True when an environment proxy would intercept the loopback endpoint:
    /// http_proxy/all_proxy is set and no_proxy does not exempt the endpoint's
    /// host. CI has no proxy configured, so the test runs there.
    fn loopback_intercepted_by_proxy() -> bool {
        let proxy_set = ["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"]
            .iter()
            .any(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false));
        if !proxy_set {
            return false;
        }
        let no_proxy = std::env::var("no_proxy")
            .or_else(|_| std::env::var("NO_PROXY"))
            .unwrap_or_default();
        // The stalled server binds 127.0.0.1, so only a wildcard or an exact
        // host match exempts it (reqwest matches no_proxy entries by host).
        !no_proxy
            .split(',')
            .map(str::trim)
            .any(|entry| matches!(entry, "*" | "127.0.0.1"))
    }

    #[tokio::test]
    async fn test_opendal_stalled_read_times_out() {
        // The loopback server must be reached directly. Proxies configured
        // through the http_proxy/all_proxy environment variables would answer
        // for it instead, unless no_proxy exempts the loopback host.
        if loopback_intercepted_by_proxy() {
            eprintln!("skipping test_opendal_stalled_read_times_out: proxy configured");
            return;
        }

        let endpoint = spawn_stalled_http_server();
        let config = BTreeMap::from([
            ("endpoint".to_string(), endpoint),
            ("io_timeout_ms".to_string(), "2000".to_string()),
        ]);
        let source = OpenDALSource::get_client("http", &config)
            .await
            .expect("Failed to create http client");

        let start = std::time::Instant::now();
        let result = source
            .get("http://test/stalled.txt", None, None)
            .await
            .expect("get failed");
        // The outer timeout keeps this a fast red test rather than a hung CI
        // job if the deadline layering ever regresses.
        let bytes = tokio::time::timeout(Duration::from_secs(30), result.bytes())
            .await
            .expect("stalled read must terminate")
            .expect_err("stalled read must fail, not hang");
        // Fails within the configured deadline instead of hanging forever.
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "elapsed: {bytes}"
        );
    }

    #[tokio::test]
    async fn test_opendal_invalid_timeout_config_fails_client_creation() {
        let config = BTreeMap::from([("io_timeout_ms".to_string(), "soon".to_string())]);
        let err = config_duration_ms("memory", &config, "io_timeout_ms")
            .expect_err("malformed value must be rejected");
        assert!(format!("{err:?}").contains("io_timeout_ms"));
        assert!(format!("{err:?}").contains("milliseconds"));
        assert!(OpenDALSource::get_client("memory", &config).await.is_err());

        for bad in ["0", "86400001"] {
            let config = BTreeMap::from([("timeout_ms".to_string(), bad.to_string())]);
            assert!(
                config_duration_ms("memory", &config, "timeout_ms").is_err(),
                "out-of-range value '{bad}' must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn test_opendal_fs_multipart_write() {
        let dir = tempfile::tempdir().expect("tempdir failed");
        let config = BTreeMap::from([(
            "root".to_string(),
            dir.path().to_str().expect("utf-8 tempdir path").to_string(),
        )]);
        let source = OpenDALSource::get_client("fs", &config)
            .await
            .expect("Failed to create fs client");

        let mut writer = source
            .clone()
            .create_multipart_writer("fs://localhost/multipart.bin")
            .await
            .expect("writer creation failed")
            .expect("expected a multipart writer");
        writer
            .put_part(Bytes::from_static(b"hello "))
            .await
            .expect("put_part failed");
        writer
            .put_part(Bytes::from_static(b"world"))
            .await
            .expect("put_part failed");
        writer.complete().await.expect("complete failed");

        // Verify with the local filesystem directly rather than reading back
        // through the same OpenDAL implementation that wrote the data, so
        // writer and reader defects cannot mask each other.
        let written = std::fs::read(dir.path().join("multipart.bin")).expect("file missing");
        assert_eq!(written, b"hello world");
    }
}
