use std::{
    any::Any,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use common_io_config::TosConfig;
use common_runtime::get_io_pool_num_threads;
use futures::{future::BoxFuture, stream::BoxStream};
use snafu::{IntoError, ResultExt, Snafu};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_stream::Stream;
use url::{ParseError, Position, Url};
use ve_tos_rust_sdk::{
    asynchronous::{
        object::ObjectAPI,
        tos,
        tos::{AsyncRuntime, TosClientImpl},
    },
    credential::{CommonCredentials, CommonCredentialsProvider},
    error::{GenericError, TosError},
    object::{
        GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListObjectsType2Input,
        ListObjectsType2Output, PutObjectFromBufferInput,
    },
};

macro_rules! set_retry_header {
    ($request:expr, $attempt:expr, $max_retries:expr) => {
        if $attempt > 0 {
            $request.set_request_header(HashMap::from([(
                HEADER_SDK_RETRY_COUNT.to_string(),
                format!("attempt={}; max={}", $attempt, $max_retries),
            )]));
        }
    };
}

use crate::{
    FileMetadata, GetRange, GetResult, IOStatsRef, InvalidRangeRequestSnafu, ObjectSource, Result,
    SourceType,
    object_io::{FileType, LSResult},
    stream_utils::io_stats_on_bytestream,
};

const DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
const DEFAULT_RETRYABLE_ERROR_MSGS: &[&str] =
    &["timeout", "broken pipe", "throttl", "qpslimit", "ratelimit"];
pub(crate) const BASE_DELAY_MS: u64 = 100;
pub(crate) const MAX_DELAY_MS: u64 = 10000;
pub(crate) const HEADER_SDK_RETRY_COUNT: &str = "x-sdk-retry-count";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to create tos client with endpoint {} error: {}",
        endpoint,
        source
    ))]
    ClientCreation { endpoint: String, source: TosError },

    #[snafu(display("Unsupported scheme: {} in URL: \"{}\"", scheme, path))]
    UnSupportedScheme { scheme: String, path: String },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl { path: String, source: ParseError },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile { path: String, source: TosError },

    #[snafu(display("Unable to put {}: {}", path, source))]
    UnableToPutFile { path: String, source: TosError },

    #[snafu(display("Unable to list {}: {}", path, source))]
    UnableToListObjects { path: String, source: TosError },

    #[snafu(display("Unable to grab semaphore. {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },

    #[snafu(display("Operation failed after {} retries: {}", retries, err_msg))]
    MaxRetriesExceeded { retries: u32, err_msg: String },
}

#[allow(clippy::fallible_impl_from)]
impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::ClientCreation {
                endpoint: _,
                source,
            } => Self::UnableToCreateClient {
                store: SourceType::Tos,
                source: Box::new(source),
            },
            Error::InvalidUrl { path, source } => Self::InvalidUrl { path, source },
            Error::UnableToOpenFile { path, source } => Self::UnableToOpenFile {
                path,
                source: Box::new(source),
            },
            Error::MaxRetriesExceeded { retries, err_msg } => Self::Generic {
                store: SourceType::Tos,
                source: Error::MaxRetriesExceeded { retries, err_msg }.into(),
            },
            err => Self::Generic {
                store: SourceType::Tos,
                source: err.into(),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct TokioRuntime {}

#[async_trait]
impl AsyncRuntime for TokioRuntime {
    type JoinError = tokio::task::JoinError;
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    fn spawn<'a, F>(&self, future: F) -> BoxFuture<'a, Result<F::Output, Self::JoinError>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Box::pin(Handle::current().spawn(future))
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        Handle::current().block_on(future)
    }
}

type TosClient =
    TosClientImpl<CommonCredentialsProvider<CommonCredentials>, CommonCredentials, TokioRuntime>;

#[derive(Debug)]
pub struct TosSource {
    client: TosClient,
    connection_pool_sema: Arc<Semaphore>,
    config: TosConfig,
}

impl TosSource {
    async fn retry_operation<T, F, Fut, E>(
        &self,
        operation: F,
        operation_name: &str,
        path: &str,
        max_retries: u32,
        error_converter: impl Fn(TosError) -> E,
    ) -> Result<T, E>
    where
        F: Fn(u32) -> Fut,
        Fut: Future<Output = Result<T, TosError>>,
        E: From<Error>,
    {
        let mut last_error = None;

        for attempt in 0..=max_retries {
            match operation(attempt).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error.clone());

                    let (retry_after, need_retry) = Self::is_retryable_error(&error);
                    if need_retry {
                        if attempt < max_retries {
                            let delay = Self::cal_sleep_duration(attempt, retry_after).await;
                            log::warn!(
                                "TOS {} operation failed for {} (attempt {}/{}): {}. Retrying in {:?}",
                                operation_name,
                                path,
                                attempt + 1,
                                max_retries + 1,
                                error,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                        } else {
                            return Err(Error::MaxRetriesExceeded {
                                retries: max_retries,
                                err_msg: last_error.map(|e| e.to_string()).unwrap_or_default(),
                            }
                            .into());
                        }
                    } else {
                        return Err(error_converter(error));
                    }
                }
            }
        }

        Err(Error::MaxRetriesExceeded {
            retries: max_retries,
            err_msg: last_error.map(|e| e.to_string()).unwrap_or_default(),
        }
        .into())
    }

    async fn cal_sleep_duration(attempt: u32, retry_after: isize) -> Duration {
        let mut delay = BASE_DELAY_MS * 2u64.pow(attempt + 1);
        if delay > MAX_DELAY_MS {
            delay = MAX_DELAY_MS;
        }
        let retry_after = if retry_after > 0 {
            retry_after as u64 * 1000
        } else {
            0
        };
        if retry_after > delay {
            delay = retry_after;
        }
        Duration::from_millis(delay)
    }

    // Returns the number of retries to wait before retrying the operation, and whether the operation is retryable.
    fn is_retryable_error(error: &TosError) -> (isize, bool) {
        match error {
            TosError::TosClientError { cause, .. } => {
                if let Some(error) = cause.as_ref() {
                    match error {
                        GenericError::HttpRequestError(_) | GenericError::IoError(_) => (0, true),
                        GenericError::DefaultError(msg) => {
                            if DEFAULT_RETRYABLE_ERROR_MSGS
                                .iter()
                                .any(|&s| msg.to_lowercase().contains(s))
                            {
                                (0, true)
                            } else {
                                (0, false)
                            }
                        }
                        _ => (0, false),
                    }
                } else {
                    (0, false)
                }
            }
            TosError::TosServerError {
                status_code,
                header,
                ..
            } => {
                let retry_after = header
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("retry-after"))
                    .and_then(|(_, v)| v.parse::<isize>().ok())
                    .unwrap_or(0);

                let should_retry = *status_code >= 500
                    || *status_code == 429
                    || *status_code == 408
                    || DEFAULT_RETRYABLE_ERROR_MSGS
                        .iter()
                        .any(|&s| error.to_string().to_lowercase().contains(s));

                (retry_after, should_retry)
            }
        }
    }

    pub async fn get_client(config: &TosConfig) -> Result<Arc<Self>> {
        let (endpoint, region) = config.endpoint_and_region();

        let mut builder = tos::builder::<TokioRuntime>()
            .max_connections(config.max_concurrent_requests as isize)
            .connection_timeout(config.connect_timeout_ms as isize)
            .request_timeout(config.read_timeout_ms as isize)
            .max_retry_count(0) // disable the retry logical of SDK
            .region(region)
            .endpoint(endpoint.clone());

        if let Some(access_key) = config.access_key.clone() {
            builder = builder.ak(access_key);
        }

        if let Some(secret_key) = config.secret_key.clone() {
            builder = builder.sk(secret_key.as_string().clone());
        }

        if let Some(session_token) = config.security_token.clone() {
            builder = builder.security_token(session_token.as_string().clone());
        }

        let client = builder
            .max_retry_count(config.max_retries as isize)
            .build()
            .with_context(|_| ClientCreationSnafu { endpoint })?;

        let connection_pool_sema = Arc::new(Semaphore::new(
            (config.max_connections_per_io_thread as usize)
                * get_io_pool_num_threads().expect("Should be running in tokio pool"),
        ));

        Ok(Arc::new(Self {
            client,
            connection_pool_sema,
            config: config.clone(),
        }))
    }

    fn parse_tos_url(url: &str, allow_empty_key: bool) -> Result<(String, String)> {
        let parsed = Url::parse(url).with_context(|_| InvalidUrlSnafu {
            path: url.to_string(),
        })?;

        if parsed.scheme() != "tos" {
            return Err(Error::UnSupportedScheme {
                scheme: parsed.scheme().to_string(),
                path: url.to_string(),
            }
            .into());
        }

        let bucket = parsed.host_str().ok_or_else(|| Error::InvalidUrl {
            path: parsed.to_string(),
            source: ParseError::EmptyHost,
        })?;

        let bucket_scheme_len = parsed[..Position::AfterHost].len();
        let key = url[bucket_scheme_len..].trim_start_matches(DELIMITER);

        if !allow_empty_key && key.is_empty() {
            return Err(super::Error::NotAFile {
                path: parsed.to_string(),
            }
            .into());
        }

        Ok((bucket.to_string(), key.to_string()))
    }

    async fn get_impl(
        &self,
        permit: OwnedSemaphorePermit,
        uri: &str,
        range: Option<GetRange>,
    ) -> Result<GetResult> {
        let (bucket, key) = Self::parse_tos_url(uri, false)?;

        if let Some(range) = range.as_ref() {
            range.validate().context(InvalidRangeRequestSnafu)?;
        }

        let response: GetObjectOutput = self
            .retry_operation(
                |attempt| {
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let range = range.clone();
                    let client = &self.client;
                    let max_retries = self.config.max_retries;
                    async move {
                        let mut request = GetObjectInput::new(bucket, key);
                        if let Some(ref range) = range {
                            request.set_range(range.to_string());
                        }
                        set_retry_header!(request, attempt, max_retries);
                        client.get_object(&request).await
                    }
                },
                "get_object",
                uri,
                self.config.max_retries,
                |err| Error::UnableToOpenFile {
                    path: uri.to_string(),
                    source: err,
                },
            )
            .await?;

        let size = response.content_length() as usize;

        struct WrapperStream {
            stream: GetObjectOutput,
            uri: String,
        }

        impl Stream for WrapperStream {
            type Item = Result<Bytes>;
            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                Pin::new(&mut self.stream).poll_next(cx).map_err(|e| {
                    super::UnableToReadBytesSnafu {
                        path: self.uri.clone(),
                    }
                    .into_error(e)
                    .into()
                })
            }
        }

        let stream = Box::pin(WrapperStream {
            stream: response,
            uri: uri.to_owned(),
        });

        Ok(GetResult::Stream(stream, Some(size), Some(permit), None))
    }

    async fn put_impl(&self, _permit: OwnedSemaphorePermit, uri: &str, data: Bytes) -> Result<()> {
        let (bucket, key) = Self::parse_tos_url(uri, false)?;

        self.retry_operation(
            |attempt| {
                let bucket = bucket.clone();
                let key = key.clone();
                let data = data.clone();
                let client = &self.client;
                let max_retries = self.config.max_retries;
                async move {
                    let mut request = PutObjectFromBufferInput::new_with_content(bucket, key, data);
                    set_retry_header!(request, attempt, max_retries);
                    client.put_object_from_buffer(&request).await
                }
            },
            "put_object",
            uri,
            self.config.max_retries,
            |err| Error::UnableToPutFile {
                path: uri.to_string(),
                source: err,
            },
        )
        .await?;

        Ok(())
    }

    async fn get_size_impl(&self, _permit: OwnedSemaphorePermit, uri: &str) -> Result<usize> {
        let (bucket, key) = Self::parse_tos_url(uri, false)?;

        let response: HeadObjectOutput = self
            .retry_operation(
                |attempt| {
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let client = &self.client;
                    let max_retries = self.config.max_retries;
                    async move {
                        let mut request = HeadObjectInput::new(bucket, key);
                        set_retry_header!(request, attempt, max_retries);
                        client.head_object(&request).await
                    }
                },
                "head_object",
                uri,
                self.config.max_retries,
                |err| Error::UnableToOpenFile {
                    path: uri.to_string(),
                    source: err,
                },
            )
            .await?;

        Ok(response.content_length() as usize)
    }

    async fn list_impl(
        &self,
        _permit: OwnedSemaphorePermit,
        bucket: &str,
        key: &str,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        page_size: Option<i32>,
    ) -> Result<LSResult> {
        let bucket = bucket.to_string();
        let key = key.to_string();

        let result = self
            .retry_operation(
                |attempt| {
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let delimiter = delimiter.clone();
                    let continuation_token = continuation_token.clone();
                    let client = &self.client;
                    let max_retries = self.config.max_retries;
                    async move {
                        let mut request = ListObjectsType2Input::new(&bucket);
                        request.set_prefix(&key);
                        if let Some(ref delimiter) = delimiter {
                            request.set_delimiter(delimiter);
                        }
                        if let Some(ref continuation_token) = continuation_token {
                            request.set_continuation_token(continuation_token);
                        }
                        if let Some(page_size) = page_size {
                            request.set_max_keys(page_size as isize);
                        }
                        set_retry_header!(request, attempt, max_retries);
                        client.list_objects_type2(&request).await
                    }
                },
                "list_objects_type2",
                &format!("tos://{bucket}/{key}"),
                self.config.max_retries,
                |err| Error::UnableToListObjects {
                    path: format!("tos://{bucket}/{key}"),
                    source: err,
                },
            )
            .await
            .map(|r: ListObjectsType2Output| {
                let dirs = r.common_prefixes();
                let files = r.contents();

                let files = dirs
                    .iter()
                    .map(|prefix| FileMetadata {
                        filepath: format!("tos://{}/{}", bucket, prefix.prefix()),
                        size: None,
                        filetype: FileType::Directory,
                    })
                    .chain(files.iter().map(|f| FileMetadata {
                        filepath: format!("tos://{}/{}", bucket, f.key()),
                        size: Some(f.size() as u64),
                        filetype: FileType::File,
                    }))
                    .collect();
                let continuation_token = (!r.next_continuation_token().is_empty())
                    .then(|| r.next_continuation_token().to_string());

                LSResult {
                    files,
                    continuation_token,
                }
            })?;
        Ok(result)
    }
}

#[async_trait]
impl ObjectSource for TosSource {
    async fn supports_range(&self, _: &str) -> Result<bool> {
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
        if io_stats.is_some() {
            if let GetResult::Stream(stream, num_bytes, permit, retry_params) = get_result {
                if let Some(is) = io_stats.as_ref() {
                    is.mark_get_requests(1);
                }
                Ok(GetResult::Stream(
                    io_stats_on_bytestream(stream, io_stats),
                    num_bytes,
                    permit,
                    retry_params,
                ))
            } else {
                panic!("This should always be a stream");
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

        let ret = self.get_size_impl(permit, uri).await?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }
        Ok(ret)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<Arc<crate::IOStatsContext>>,
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
        let (bucket, prefix) = Self::parse_tos_url(path, true)?;
        if posix {
            let prefix = if prefix.is_empty() {
                String::new()
            } else {
                format!("{}{DELIMITER}", prefix.trim_end_matches(DELIMITER))
            };

            let permit = self
                .connection_pool_sema
                .clone()
                .acquire_owned()
                .await
                .context(UnableToGrabSemaphoreSnafu)?;
            let ret = self
                .list_impl(
                    permit,
                    &bucket,
                    &prefix,
                    Some(DELIMITER.into()),
                    continuation_token.map(|s| s.to_string()),
                    page_size,
                )
                .await?;
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1);
            }

            Ok(ret)
        } else {
            let permit = self
                .connection_pool_sema
                .clone()
                .acquire_owned()
                .await
                .context(UnableToGrabSemaphoreSnafu)?;
            let ret = self
                .list_impl(
                    permit,
                    &bucket,
                    &prefix,
                    None,
                    continuation_token.map(|s| s.to_string()),
                    page_size,
                )
                .await?;
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1);
            }

            Ok(ret)
        }
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_config() -> TosConfig {
        TosConfig {
            region: Some("cn-beijing".to_string()),
            endpoint: Some("https://tos-cn-beijing.volces.com".to_string()),
            anonymous: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_tos_client_creation() {
        let config = setup_test_config();
        let client = TosSource::get_client(&config).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_parse_tos_url() {
        let url = "tos://my-bucket/path/to/file.txt";
        let (bucket, key) = TosSource::parse_tos_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.txt");

        let url = "tos://my-bucket/file.txt";
        let (bucket, key) = TosSource::parse_tos_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "file.txt");

        let url = "tos://my-bucket/";
        let (bucket, key) = TosSource::parse_tos_url(url, true).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");

        let url = "tos://my-bucket/";
        let result = TosSource::parse_tos_url(url, false);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_parse_tos_url_invalid() {
        let url = "invalid-url";
        let result = TosSource::parse_tos_url(url, true);
        assert!(result.is_err());

        let url = "http://example.com/file.txt";
        let result = TosSource::parse_tos_url(url, true);
        assert!(result.is_err());

        let url = "tos://";
        let result = TosSource::parse_tos_url(url, true);
        assert!(result.is_err());
    }
}
