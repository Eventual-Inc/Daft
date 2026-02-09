//! Tencent Cloud COS (Cloud Object Storage) support using OpenDAL
//!
//! This module implements the ObjectSource trait for Tencent Cloud COS using OpenDAL as the backend.
//! It supports reading, writing, listing, and deleting objects in COS buckets.
//!
//! # Supported URL schemes
//! - `cos://<bucket>/<key>` - Standard COS URI format
//! - `cosn://<bucket>/<key>` - Hadoop CosN connector compatible format
//!
//! # Environment Variables
//! The following environment variables are supported (following OpenDAL conventions):
//! - `COS_ENDPOINT` - COS endpoint URL
//! - `COS_SECRET_ID` - Tencent Cloud SecretId
//! - `COS_SECRET_KEY` - Tencent Cloud SecretKey
//! - `TENCENTCLOUD_SECRET_ID` - Alternative SecretId
//! - `TENCENTCLOUD_SECRET_KEY` - Alternative SecretKey
//! - `TENCENTCLOUD_SECURITY_TOKEN` - STS temporary token
//! - `TENCENTCLOUD_REGION` - Region name

use std::{any::Any, collections::HashMap, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use common_io_config::CosConfig;
use common_runtime::get_io_pool_num_threads;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use opendal::{Operator, services::Cos};
use snafu::{ResultExt, Snafu};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::{
    FileMetadata, GetRange, GetResult, IOStatsRef, ObjectSource, Result, SourceType,
    object_io::{FileType, LSResult},
    stream_utils::io_stats_on_bytestream,
};

const DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create COS operator: {}", source))]
    OperatorCreation { source: opendal::Error },

    #[snafu(display(
        "COS endpoint is required. Please provide 'endpoint' in CosConfig or set COS_ENDPOINT environment variable"
    ))]
    EndpointRequired,

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },

    #[snafu(display("Unsupported scheme: {} in URL: \"{}\"", scheme, path))]
    UnsupportedScheme { scheme: String, path: String },

    #[snafu(display("Unable to read {}: {}", path, source))]
    UnableToRead {
        path: String,
        source: opendal::Error,
    },

    #[snafu(display("Unable to write {}: {}", path, source))]
    UnableToWrite {
        path: String,
        source: opendal::Error,
    },

    #[snafu(display("Unable to stat {}: {}", path, source))]
    UnableToStat {
        path: String,
        source: opendal::Error,
    },

    #[snafu(display("Unable to list {}: {}", path, source))]
    UnableToList {
        path: String,
        source: opendal::Error,
    },

    #[snafu(display("Unable to delete {}: {}", path, source))]
    UnableToDelete {
        path: String,
        source: opendal::Error,
    },

    #[snafu(display("Unable to grab semaphore: {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },

    #[snafu(display("Not a file: \"{}\"", path))]
    NotAFile { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::OperatorCreation { source } => Self::UnableToCreateClient {
                store: SourceType::Cos,
                source: Box::new(source),
            },
            Error::InvalidUrl { path, source } => Self::InvalidUrl { path, source },
            Error::UnableToRead { path, source } => Self::UnableToOpenFile {
                path,
                source: Box::new(source),
            },
            Error::UnableToStat { path, source } => {
                if source.kind() == opendal::ErrorKind::NotFound {
                    Self::NotFound {
                        path,
                        source: Box::new(source),
                    }
                } else {
                    Self::UnableToOpenFile {
                        path,
                        source: Box::new(source),
                    }
                }
            }
            err => Self::Generic {
                store: SourceType::Cos,
                source: Box::new(err),
            },
        }
    }
}

/// COS source implementation using OpenDAL
#[derive(Debug)]
pub struct CosSource {
    /// OpenDAL operator for COS operations
    operator: Operator,
    /// Configuration
    config: CosConfig,
    /// Semaphore for connection pool limiting
    connection_pool_sema: Arc<Semaphore>,
    /// Bucket name extracted from the first URL
    bucket: String,
}

impl CosSource {
    /// Create a new COS client from configuration
    pub async fn get_client(config: &CosConfig, url: &str) -> Result<Arc<Self>> {
        // Parse URL to extract bucket
        let (bucket, _) = Self::parse_cos_url(url, true)?;

        // Build configuration map for OpenDAL
        let mut config_map: HashMap<String, String> = HashMap::new();

        // Start with environment variables as base configuration
        // OpenDAL COS supports: COS_*, TENCENTCLOUD_*
        for (key, value) in std::env::vars() {
            if key.starts_with("COS_") || key.starts_with("TENCENTCLOUD_") {
                let config_key = key
                    .to_lowercase()
                    .replace("cos_", "")
                    .replace("tencentcloud_", "");
                config_map.insert(config_key, value);
            }
        }

        // Set bucket
        config_map.insert("bucket".to_string(), bucket.clone());

        // Get endpoint and region from config
        let (endpoint, region) = config.endpoint_and_region();

        if let Some(ep) = endpoint {
            config_map.insert("endpoint".to_string(), ep);
        }

        config_map.insert("region".to_string(), region);

        // Override with explicit config values
        if let Some(secret_id) = &config.secret_id {
            config_map.insert("secret_id".to_string(), secret_id.clone());
        }

        if let Some(secret_key) = &config.secret_key {
            config_map.insert("secret_key".to_string(), secret_key.as_string().clone());
        }

        if let Some(security_token) = &config.security_token {
            config_map.insert(
                "security_token".to_string(),
                security_token.as_string().clone(),
            );
        }

        // Allow loading from environment variables
        // This is important for configurations like TENCENTCLOUD_SECURITY_TOKEN, TENCENTCLOUD_REGION
        config_map.insert("disable_config_load".to_string(), "false".to_string());

        // Validate endpoint is present
        if !config_map.contains_key("endpoint") {
            return Err(Error::EndpointRequired.into());
        }

        // Create OpenDAL operator
        let operator = Operator::from_iter::<Cos>(config_map)
            .context(OperatorCreationSnafu)?
            .finish();

        // Calculate semaphore permits
        let num_permits = (config.max_connections_per_io_thread as usize)
            * get_io_pool_num_threads().unwrap_or(1);
        let connection_pool_sema = Arc::new(Semaphore::new(num_permits));

        Ok(Arc::new(Self {
            operator,
            config: config.clone(),
            connection_pool_sema,
            bucket,
        }))
    }

    /// Get the bucket name (used in tests and for debugging)
    #[allow(dead_code)]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the configuration (used in tests and for debugging)
    #[allow(dead_code)]
    pub fn config(&self) -> &CosConfig {
        &self.config
    }

    /// Parse COS URL and extract bucket and key
    /// Supports both `cos://bucket/key` and `cosn://bucket/key` formats
    fn parse_cos_url(url: &str, allow_empty_key: bool) -> Result<(String, String)> {
        let parsed = Url::parse(url).map_err(|e| Error::InvalidUrl {
            path: url.to_string(),
            source: e,
        })?;

        let scheme = parsed.scheme().to_lowercase();
        if scheme != "cos" && scheme != "cosn" {
            return Err(Error::UnsupportedScheme {
                scheme,
                path: url.to_string(),
            }
            .into());
        }

        let bucket = parsed
            .host_str()
            .ok_or_else(|| Error::InvalidUrl {
                path: url.to_string(),
                source: url::ParseError::EmptyHost,
            })?
            .to_string();

        let key = parsed.path().trim_start_matches('/').to_string();

        if !allow_empty_key && key.is_empty() {
            return Err(Error::NotAFile {
                path: url.to_string(),
            }
            .into());
        }

        Ok((bucket, key))
    }

    /// Convert GetRange to OpenDAL range
    fn get_range_to_opendal_range(range: &GetRange, size: u64) -> Range<u64> {
        match range {
            GetRange::Bounded(r) => r.start as u64..r.end as u64,
            GetRange::Offset(o) => *o as u64..size,
            GetRange::Suffix(n) => {
                let start = size.saturating_sub(*n as u64);
                start..size
            }
        }
    }

    async fn get_impl(
        &self,
        permit: OwnedSemaphorePermit,
        uri: &str,
        range: Option<GetRange>,
    ) -> Result<GetResult> {
        let (_, key) = Self::parse_cos_url(uri, false)?;

        log::debug!(
            "COS get_impl: bucket={}, key={}, max_retries={}",
            self.bucket,
            key,
            self.config.max_retries
        );

        // Get file size first if we need it for range calculation
        let size = if range.is_some() {
            let meta = self
                .operator
                .stat(&key)
                .await
                .context(UnableToStatSnafu { path: uri })?;
            Some(meta.content_length())
        } else {
            None
        };

        // Create reader with optional range
        let reader = if let Some(ref r) = range {
            let file_size = size.unwrap_or(0);
            let opendal_range = Self::get_range_to_opendal_range(r, file_size);
            self.operator
                .read_with(&key)
                .range(opendal_range)
                .await
                .context(UnableToReadSnafu { path: uri })?
        } else {
            self.operator
                .read(&key)
                .await
                .context(UnableToReadSnafu { path: uri })?
        };

        // Convert Buffer to stream
        let bytes = reader.to_bytes();
        let stream = futures::stream::once(async move { Ok(bytes) }).boxed();

        let result_size = size.map(|s| s as usize);

        Ok(GetResult::Stream(stream, result_size, Some(permit), None))
    }

    async fn put_impl(&self, _permit: OwnedSemaphorePermit, uri: &str, data: Bytes) -> Result<()> {
        let (_, key) = Self::parse_cos_url(uri, false)?;

        self.operator
            .write(&key, data)
            .await
            .context(UnableToWriteSnafu { path: uri })?;

        Ok(())
    }

    async fn get_size_impl(&self, _permit: OwnedSemaphorePermit, uri: &str) -> Result<usize> {
        let (_, key) = Self::parse_cos_url(uri, false)?;

        let meta = self
            .operator
            .stat(&key)
            .await
            .context(UnableToStatSnafu { path: uri })?;

        Ok(meta.content_length() as usize)
    }

    async fn list_impl(
        &self,
        _permit: OwnedSemaphorePermit,
        prefix: &str,
        posix: bool,
        _continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> Result<LSResult> {
        let (bucket, key) = Self::parse_cos_url(prefix, true)?;

        let prefix_path = if key.is_empty() {
            String::new()
        } else if posix {
            // For POSIX-style listing, ensure prefix ends with delimiter
            if key.ends_with(DELIMITER) {
                key
            } else {
                format!("{}{}", key, DELIMITER)
            }
        } else {
            key
        };

        // When posix is true, we want non-recursive listing (only current directory level)
        // When posix is false, we want recursive listing (all files under prefix)
        let lister = self
            .operator
            .lister_with(&prefix_path)
            .recursive(!posix)
            .await
            .context(UnableToListSnafu { path: prefix })?;

        // Note: OpenDAL doesn't directly support continuation tokens in the same way
        // We'll collect results and handle pagination at a higher level
        let limit = page_size.unwrap_or(1000) as usize;

        let entries: Vec<_> = lister
            .take(limit)
            .try_collect()
            .await
            .context(UnableToListSnafu { path: prefix })?;

        let scheme = if prefix.starts_with("cosn://") {
            "cosn"
        } else {
            "cos"
        };

        let files: Vec<FileMetadata> = entries
            .into_iter()
            .map(|entry| {
                let path = entry.path();
                let filepath = format!("{}://{}/{}", scheme, bucket, path.trim_start_matches('/'));

                let meta = entry.metadata();
                let filetype = if meta.is_dir() {
                    FileType::Directory
                } else {
                    FileType::File
                };

                let size = if filetype == FileType::File {
                    Some(meta.content_length())
                } else {
                    None
                };

                FileMetadata {
                    filepath,
                    size,
                    filetype,
                }
            })
            .collect();

        // OpenDAL handles pagination internally, so we don't have a continuation token
        // For now, we return None
        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }
}

#[async_trait]
impl ObjectSource for CosSource {
    async fn supports_range(&self, _uri: &str) -> Result<bool> {
        Ok(true)
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<GetResult> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let get_result = self.get_impl(permit, uri, range).await?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1);
        }

        if io_stats.is_some() {
            if let GetResult::Stream(stream, num_bytes, permit, retry_params) = get_result {
                Ok(GetResult::Stream(
                    io_stats_on_bytestream(stream, io_stats),
                    num_bytes,
                    permit,
                    retry_params,
                ))
            } else {
                Ok(get_result)
            }
        } else {
            Ok(get_result)
        }
    }

    async fn put(&self, uri: &str, data: Bytes, io_stats: Option<IOStatsRef>) -> Result<()> {
        let data_len = data.len();
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        self.put_impl(permit, uri, data).await?;

        if let Some(io_stats) = io_stats {
            io_stats.as_ref().mark_put_requests(1);
            io_stats.as_ref().mark_bytes_uploaded(data_len);
        }

        Ok(())
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> Result<usize> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let size = self.get_size_impl(permit, uri).await?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }

        Ok(size)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        _file_format: Option<crate::FileFormat>,
    ) -> Result<BoxStream<'static, Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        // Ensure fanout_limit is not None to prevent runaway concurrency
        let fanout_limit = fanout_limit.or(Some(DEFAULT_GLOB_FANOUT_LIMIT));

        glob(
            self,
            glob_path,
            fanout_limit,
            page_size.or(Some(1000)),
            limit,
            io_stats,
        )
        .await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<LSResult> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let result = self
            .list_impl(permit, path, posix, continuation_token, page_size)
            .await?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }

        Ok(result)
    }

    async fn delete(&self, uri: &str, io_stats: Option<IOStatsRef>) -> Result<()> {
        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let (_, key) = Self::parse_cos_url(uri, false)?;

        self.operator
            .delete(&key)
            .await
            .context(UnableToDeleteSnafu { path: uri })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_delete_requests(1);
        }

        Ok(())
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use bytes::Bytes;
    use common_io_config::ObfuscatedString;
    use rand::{RngCore, thread_rng};

    use super::*;
    use crate::integrations::test_full_get;

    struct ClientGuard {
        client: Arc<CosSource>,
        uris: Vec<String>,
    }

    impl ClientGuard {
        fn new(client: Arc<CosSource>, uris: Vec<String>) -> Self {
            Self { client, uris }
        }

        async fn cleanup(self) {
            for uri in self.uris.clone() {
                let _ = self.client.delete(&uri, None).await;
            }
        }
    }

    impl Drop for ClientGuard {
        fn drop(&mut self) {
            let client = self.client.clone();
            let uris = self.uris.clone();
            // Try the best effort to clean up the object after the test.
            let _ = tokio::runtime::Handle::current().spawn(async move {
                for uri in uris {
                    let _ = client.delete(&uri, None).await;
                }
            });
        }
    }

    fn setup_online_test_config() -> Option<(CosConfig, String)> {
        let bucket = env::var("COS_TEST_BUCKET").ok();
        let secret_id = env::var("COS_SECRET_ID")
            .or_else(|_| env::var("TENCENTCLOUD_SECRET_ID"))
            .ok();
        let secret_key = env::var("COS_SECRET_KEY")
            .or_else(|_| env::var("TENCENTCLOUD_SECRET_KEY"))
            .ok();

        if bucket.is_none() || secret_id.is_none() || secret_key.is_none() {
            None
        } else {
            Some((
                CosConfig {
                    region: Some(
                        env::var("COS_REGION")
                            .or_else(|_| env::var("TENCENTCLOUD_REGION"))
                            .unwrap_or("ap-guangzhou".to_string()),
                    ),
                    endpoint: env::var("COS_ENDPOINT").ok(),
                    secret_id,
                    secret_key: secret_key.map(ObfuscatedString::from),
                    security_token: env::var("COS_SECURITY_TOKEN")
                        .or_else(|_| env::var("TENCENTCLOUD_SECURITY_TOKEN"))
                        .ok()
                        .map(ObfuscatedString::from),
                    ..Default::default()
                },
                bucket.unwrap(),
            ))
        }
    }

    #[test]
    fn test_parse_cos_url() {
        // Test standard cos:// URL
        let (bucket, key) =
            CosSource::parse_cos_url("cos://my-bucket/path/to/file.txt", false).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.txt");

        // Test cosn:// URL (Hadoop compatible)
        let (bucket, key) =
            CosSource::parse_cos_url("cosn://my-bucket/path/to/file.txt", false).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.txt");

        // Test URL with empty key (allowed)
        let (bucket, key) = CosSource::parse_cos_url("cos://my-bucket/", true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");

        // Test URL with empty key (not allowed)
        let result = CosSource::parse_cos_url("cos://my-bucket/", false);
        assert!(result.is_err());

        // Test invalid scheme
        let result = CosSource::parse_cos_url("s3://my-bucket/key", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cos_url_invalid() {
        let result = CosSource::parse_cos_url("invalid-url", true);
        assert!(result.is_err());

        let result = CosSource::parse_cos_url("http://example.com/file.txt", true);
        assert!(result.is_err());

        let result = CosSource::parse_cos_url("cos://", true);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_range_to_opendal_range() {
        let file_size = 1000u64;

        // Test bounded range
        let range = GetRange::Bounded(100..500);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 100..500);

        // Test offset range
        let range = GetRange::Offset(200);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 200..1000);

        // Test suffix range
        let range = GetRange::Suffix(100);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 900..1000);
    }

    #[tokio::test]
    async fn test_cos_client_creation() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let uri = format!("cos://{}/test.txt", bucket);
        let client = CosSource::get_client(&cfg, &uri).await;
        assert!(client.is_ok());

        let client = client.unwrap();
        // Test bucket() getter
        assert_eq!(client.bucket(), bucket);
        // Test config() getter - verify region is set
        assert!(client.config().region.is_some());
    }

    #[tokio::test]
    async fn test_full_get_from_cos() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let uri = format!(
            "cos://{}/{}/hello.txt",
            bucket,
            generate_test_object_prefix()
        );

        let guard = build_client_guard(&cfg, &bucket, vec![&uri]).await;

        let data = random_vec(200);
        guard.client.put(&uri, data.clone(), None).await.unwrap();

        let res = test_full_get(guard.client.clone(), &uri, &data).await;

        guard.cleanup().await;
        res.unwrap()
    }

    #[tokio::test]
    async fn test_full_ls_from_cos() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let prefix = format!("cos://{}/{}", bucket, generate_test_object_prefix());
        let uri1 = format!("{}/hello-1.txt", prefix);
        let uri2 = format!("{}/hello-2.txt", prefix);
        let guard = build_client_guard(&cfg, &bucket, vec![&uri1, &uri2]).await;

        // list empty prefix
        let res = guard
            .client
            .ls(&prefix, true, None, None, None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 0);
        assert!(res.continuation_token.is_none());

        // create two files
        let data = random_vec(200);
        guard.client.put(&uri1, data.clone(), None).await.unwrap();
        guard.client.put(&uri2, data.clone(), None).await.unwrap();

        // list total files
        let res = guard
            .client
            .ls(&prefix, true, None, None, None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 2);
        assert!(res.continuation_token.is_none());

        // list only one file with page_size=1
        let res = guard
            .client
            .ls(&prefix, true, None, Some(1), None)
            .await
            .unwrap();
        assert_eq!(res.files.len(), 1);
        // Note: OpenDAL handles pagination internally, continuation_token may or may not be present

        guard.cleanup().await;
    }

    #[tokio::test]
    async fn test_get_size_from_cos() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let uri = format!(
            "cos://{}/{}/size-test.txt",
            bucket,
            generate_test_object_prefix()
        );

        let guard = build_client_guard(&cfg, &bucket, vec![&uri]).await;

        let data = random_vec(500);
        guard.client.put(&uri, data.clone(), None).await.unwrap();

        let size = guard.client.get_size(&uri, None).await.unwrap();
        assert_eq!(size, 500);

        guard.cleanup().await;
    }

    #[tokio::test]
    async fn test_delete_from_cos() {
        let (cfg, bucket) = match setup_online_test_config() {
            Some(c) => c,
            None => return,
        };
        let uri = format!(
            "cos://{}/{}/delete-test.txt",
            bucket,
            generate_test_object_prefix()
        );

        let guard = build_client_guard(&cfg, &bucket, vec![&uri]).await;

        let data = random_vec(100);
        guard.client.put(&uri, data.clone(), None).await.unwrap();

        // Verify file exists
        let size = guard.client.get_size(&uri, None).await;
        assert!(size.is_ok());

        // Delete and verify
        guard.client.delete(&uri, None).await.unwrap();
        let size = guard.client.get_size(&uri, None).await;
        assert!(size.is_err());
    }

    fn generate_test_object_prefix() -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("daft-tests/{}/{}", std::process::id(), ts)
    }

    fn random_vec(n: usize) -> Bytes {
        let mut buf = vec![0u8; n];
        thread_rng().fill_bytes(&mut buf);
        Bytes::from(buf)
    }

    async fn build_client_guard(cfg: &CosConfig, bucket: &str, uris: Vec<&str>) -> ClientGuard {
        let uri = format!("cos://{}/test.txt", bucket);
        let client = CosSource::get_client(cfg, &uri).await.unwrap();
        ClientGuard::new(client, uris.iter().map(|s| s.to_string()).collect())
    }

    // Additional unit tests (no network required)

    #[test]
    fn test_error_display() {
        // Test error display formatting
        let err = Error::EndpointRequired;
        let msg = format!("{}", err);
        assert!(msg.contains("endpoint"));

        let err = Error::InvalidUrl {
            path: "invalid".to_string(),
            source: url::ParseError::EmptyHost,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("invalid"));

        let err = Error::UnsupportedScheme {
            scheme: "s3".to_string(),
            path: "s3://bucket/key".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("s3"));

        let err = Error::NotAFile {
            path: "cos://bucket/".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("cos://bucket/"));
    }

    #[test]
    fn test_error_conversion() {
        // Test Error -> super::Error conversion
        let err = Error::EndpointRequired;
        let super_err: super::super::Error = err.into();
        match super_err {
            super::super::Error::Generic { store, .. } => {
                assert_eq!(store, crate::SourceType::Cos);
            }
            _ => panic!("Expected Generic error"),
        }

        let err = Error::InvalidUrl {
            path: "invalid".to_string(),
            source: url::ParseError::EmptyHost,
        };
        let super_err: super::super::Error = err.into();
        match super_err {
            super::super::Error::InvalidUrl { path, .. } => {
                assert_eq!(path, "invalid");
            }
            _ => panic!("Expected InvalidUrl error"),
        }
    }

    #[test]
    fn test_parse_cos_url_root() {
        // Test parsing root bucket URL
        let (bucket, key) = CosSource::parse_cos_url("cos://bucket", true).unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "");
    }

    #[test]
    fn test_parse_cos_url_nested_path() {
        // Test parsing deeply nested paths
        let (bucket, key) =
            CosSource::parse_cos_url("cos://bucket/a/b/c/d/e/file.txt", false).unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "a/b/c/d/e/file.txt");
    }

    #[test]
    fn test_parse_cosn_url() {
        // Test cosn:// scheme specifically
        let (bucket, key) =
            CosSource::parse_cos_url("cosn://hadoop-bucket/data/file.parquet", false).unwrap();
        assert_eq!(bucket, "hadoop-bucket");
        assert_eq!(key, "data/file.parquet");
    }

    #[test]
    fn test_get_range_edge_cases() {
        // Test edge cases for range conversion
        let file_size = 100u64;

        // Suffix larger than file
        let range = GetRange::Suffix(200);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 0..100);

        // Offset at end
        let range = GetRange::Offset(100);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 100..100);

        // Zero-length bounded range
        let range = GetRange::Bounded(50..50);
        let opendal_range = CosSource::get_range_to_opendal_range(&range, file_size);
        assert_eq!(opendal_range, 50..50);
    }

    #[test]
    fn test_delimiter_constant() {
        assert_eq!(DELIMITER, "/");
    }

    #[test]
    fn test_default_glob_fanout_limit() {
        assert_eq!(DEFAULT_GLOB_FANOUT_LIMIT, 1024);
    }
}
