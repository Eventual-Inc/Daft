use std::{any::Any, ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use common_io_config::GCSConfig;
use common_runtime::get_io_pool_num_threads;
use futures::{stream::BoxStream, TryStreamExt};
use google_cloud_storage::{
    client::{google_cloud_auth::credentials::CredentialsFile, Client, ClientConfig},
    http::{
        objects::{get::GetObjectRequest, list::ListObjectsRequest},
        Error as GError,
    },
};
use google_cloud_token::{TokenSource, TokenSourceProvider};
use regex::Regex;
use snafu::{IntoError, ResultExt, Snafu};
use tokio::sync::Semaphore;

use crate::{
    object_io::{FileMetadata, FileType, LSResult, ObjectSource},
    retry::{ExponentialBackoff, RetryError},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
    FileFormat, GetResult,
};

const GCS_DELIMITER: &str = "/";
const GCS_SCHEME: &str = "gs";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile { path: String, source: GError },

    #[snafu(display("Unable to list objects: \"{}\"", path))]
    UnableToListObjects { path: String, source: GError },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes { path: String, source: GError },

    #[snafu(display("Unable to load Credentials: {}", source))]
    UnableToLoadCredentials {
        source: google_cloud_storage::client::google_cloud_auth::error::Error,
    },
    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },
    #[snafu(display("Not a File: \"{}\"", path))]
    NotFound { path: String },
    #[snafu(display("Unable to grab semaphore. {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },

    #[snafu(display("Unable to create Http Client {}", source))]
    UnableToCreateClient {
        source: reqwest_middleware::reqwest::Error,
    },
}

#[allow(clippy::fallible_impl_from)]
impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::{
            NotAFile, NotFound, UnableToCreateClient, UnableToGrabSemaphore, UnableToListObjects,
            UnableToLoadCredentials, UnableToOpenFile, UnableToReadBytes,
        };

        fn from_reqwest_err(path: String, err: reqwest::Error) -> super::Error {
            match err.status().map(|s| s.as_u16()) {
                Some(404 | 410) => super::Error::NotFound {
                    path,
                    source: err.into(),
                },
                Some(401) => super::Error::Unauthorized {
                    store: super::SourceType::GCS,
                    path,
                    source: err.into(),
                },
                _ => {
                    if err.is_connect() {
                        super::Error::ConnectTimeout {
                            path,
                            source: err.into(),
                        }
                    } else if err.is_timeout() {
                        super::Error::ReadTimeout {
                            path,
                            source: err.into(),
                        }
                    } else {
                        super::Error::UnableToOpenFile {
                            path,
                            source: err.into(),
                        }
                    }
                }
            }
        }

        match error {
            UnableToReadBytes { path, source }
            | UnableToOpenFile { path, source }
            | UnableToListObjects { path, source } => match source {
                GError::HttpClient(err) => from_reqwest_err(path, err),
                GError::Response(err) => match err.code {
                    404 | 410 => Self::NotFound {
                        path,
                        source: err.into(),
                    },
                    401 => Self::Unauthorized {
                        store: super::SourceType::GCS,
                        path,
                        source: err.into(),
                    },
                    _ => Self::UnableToOpenFile {
                        path,
                        source: err.into(),
                    },
                },
                GError::TokenSource(err) => Self::UnableToLoadCredentials {
                    store: super::SourceType::GCS,
                    source: err,
                },
                GError::HttpMiddleware(err) if err.is::<reqwest_retry::RetryError>() => {
                    let err = err.downcast::<reqwest_retry::RetryError>().unwrap();

                    let inner_err = match err {
                        reqwest_retry::RetryError::Error(e)
                        | reqwest_retry::RetryError::WithRetries { err: e, .. } => e,
                    };

                    match inner_err {
                        reqwest_middleware::Error::Reqwest(e) => from_reqwest_err(path, e),
                        reqwest_middleware::Error::Middleware(_) => Self::UnableToOpenFile {
                            path,
                            source: inner_err.into(),
                        },
                    }
                }
                err => Self::UnableToOpenFile {
                    path,
                    source: err.into(),
                },
            },
            NotFound { ref path } => Self::NotFound {
                path: path.into(),
                source: error.into(),
            },
            UnableToLoadCredentials { source } => Self::UnableToLoadCredentials {
                store: super::SourceType::GCS,
                source: source.into(),
            },
            NotAFile { path } => Self::NotAFile { path },
            UnableToGrabSemaphore { .. } => Self::Generic {
                store: crate::SourceType::GCS,
                source: error.into(),
            },
            UnableToCreateClient { .. } => Self::UnableToCreateClient {
                store: crate::SourceType::GCS,
                source: error.into(),
            },
        }
    }
}

struct GCSClientWrapper {
    client: Client,
    /// Used to limit the concurrent connections to GCS at any given time.
    /// Acquired when we initiate a connection to GCS
    /// Released when the stream for that connection is exhausted
    connection_pool_sema: Arc<Semaphore>,
}

fn parse_raw_uri(uri: &str) -> super::Result<(&str, &str)> {
    // We use regex here instead of the more robust url crate because we do not want to handle character escaping
    // which is done by `google_cloud_storage::client::Client` already
    let re = Regex::new(r"^gs://([^/]+)(?:/(.*))?$").unwrap();

    if let Some(cap) = re.captures(uri) {
        let bucket = cap.get(1).unwrap().as_str();
        let key = cap.get(2).map_or("", |key| key.as_str());

        Ok((bucket, key))
    } else {
        Err(Error::NotAFile { path: uri.into() }.into())
    }
}

impl GCSClientWrapper {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let (bucket, key) = parse_raw_uri(uri)?;
        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        let client = &self.client;
        let req = GetObjectRequest {
            bucket: bucket.into(),
            object: key.into(),
            ..Default::default()
        };
        use google_cloud_storage::http::objects::download::Range as GRange;
        let (grange, size) = if let Some(range) = range {
            (
                GRange(Some(range.start as u64), Some(range.end as u64)),
                Some(range.len()),
            )
        } else {
            (GRange::default(), None)
        };
        let owned_uri = uri.to_string();
        let response = client
            .download_streamed_object(&req, &grange)
            .await
            .context(UnableToOpenFileSnafu {
                path: uri.to_string(),
            })?;
        let response = response.map_err(move |e| {
            UnableToReadBytesSnafu::<String> {
                path: owned_uri.clone(),
            }
            .into_error(e)
            .into()
        });
        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1);
        }
        Ok(GetResult::Stream(
            io_stats_on_bytestream(response, io_stats),
            size,
            Some(permit),
            None,
        ))
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let (bucket, key) = parse_raw_uri(uri)?;
        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }

        let _permit = self
            .connection_pool_sema
            .acquire()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let client = &self.client;
        let req = GetObjectRequest {
            bucket: bucket.into(),
            object: key.into(),
            ..Default::default()
        };

        let response = client
            .get_object(&req)
            .await
            .context(UnableToOpenFileSnafu {
                path: uri.to_string(),
            })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }
        Ok(response.size as usize)
    }
    #[allow(clippy::too_many_arguments)]
    async fn ls_impl(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<&IOStatsRef>,
    ) -> super::Result<LSResult> {
        let req = ListObjectsRequest {
            bucket: bucket.to_string(),
            prefix: Some(key.to_string()),
            end_offset: None,
            start_offset: None,
            page_token: continuation_token.map(std::string::ToString::to_string),
            delimiter: delimiter.map(std::string::ToString::to_string), // returns results in "directory mode" if delimiter is provided
            max_results: page_size,
            include_trailing_delimiter: Some(false), // This will not populate "directories" in the response's .item[]
            projection: None,
            versions: None,
            match_glob: None,
        };
        let ls_response = client
            .list_objects(&req)
            .await
            .context(UnableToListObjectsSnafu {
                path: format!("{GCS_SCHEME}://{bucket}/{key}"),
            })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }

        let response_items = ls_response.items.unwrap_or_default();
        let response_prefixes = ls_response.prefixes.unwrap_or_default();
        let files = response_items.iter().map(|obj| FileMetadata {
            filepath: format!("{GCS_SCHEME}://{}/{}", bucket, obj.name),
            size: Some(obj.size as u64),
            filetype: FileType::File,
        });
        let dirs = response_prefixes.iter().map(|pref| FileMetadata {
            filepath: format!("{GCS_SCHEME}://{bucket}/{pref}"),
            size: None,
            filetype: FileType::Directory,
        });
        Ok(LSResult {
            files: files.chain(dirs).collect(),
            continuation_token: ls_response.next_page_token,
        })
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let (bucket, key) = parse_raw_uri(path)?;

        let _permit = self
            .connection_pool_sema
            .acquire()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let client = &self.client;

        if posix {
            // Attempt to forcefully ls the key as a directory (by ensuring a "/" suffix)
            let forced_directory_key = if key.is_empty() {
                String::new()
            } else {
                format!("{}{GCS_DELIMITER}", key.trim_end_matches(GCS_DELIMITER))
            };
            let forced_directory_ls_result = self
                .ls_impl(
                    client,
                    bucket,
                    forced_directory_key.as_str(),
                    Some(GCS_DELIMITER),
                    continuation_token,
                    page_size,
                    io_stats.as_ref(),
                )
                .await?;

            // If no items were obtained, then this is actually a file and we perform a second ls to obtain just the file's
            // details as the one-and-only-one entry
            if forced_directory_ls_result.files.is_empty() {
                let mut file_result = self
                    .ls_impl(
                        client,
                        bucket,
                        key,
                        Some(GCS_DELIMITER),
                        continuation_token,
                        page_size,
                        io_stats.as_ref(),
                    )
                    .await?;

                // Only retain exact matches (since the API does prefix lists by default)
                let target_path = format!("{GCS_SCHEME}://{bucket}/{key}");
                file_result.files.retain(|fm| fm.filepath == target_path);

                // Not dir and not file, so it is missing
                if file_result.files.is_empty() {
                    return Err(Error::NotFound {
                        path: path.to_string(),
                    }
                    .into());
                }

                Ok(file_result)
            } else {
                Ok(forced_directory_ls_result)
            }
        } else {
            self.ls_impl(
                client,
                bucket,
                key,
                None, // Force a prefix-listing
                continuation_token,
                page_size,
                io_stats.as_ref(),
            )
            .await
        }
    }
}

pub struct GCSSource {
    client: GCSClientWrapper,
}

#[derive(Debug, Clone)]
struct FixedTokenSource {
    token: String,
}

impl TokenSourceProvider for FixedTokenSource {
    fn token_source(&self) -> Arc<dyn TokenSource> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl TokenSource for FixedTokenSource {
    async fn token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(format!("Bearer {0}", self.token))
    }
}

impl GCSSource {
    pub async fn get_client(config: &GCSConfig) -> super::Result<Arc<Self>> {
        let mut client_config = if config.anonymous {
            ClientConfig::default().anonymous()
        } else if let Some(creds) = &config.credentials {
            // try using credentials as file path first, then as JSON string
            let creds = match CredentialsFile::new_from_file(creds.as_string().clone()).await {
                Ok(creds) => creds,
                Err(_) => CredentialsFile::new_from_str(creds.as_string())
                    .await
                    .context(UnableToLoadCredentialsSnafu {})?,
            };

            ClientConfig::default()
                .with_credentials(creds)
                .await
                .context(UnableToLoadCredentialsSnafu {})?
        } else if let Some(token) = &config.token {
            ClientConfig {
                token_source_provider: Some(Box::new(FixedTokenSource {
                    token: token.clone(),
                })),
                ..Default::default()
            }
        } else {
            let backoff = ExponentialBackoff::default();

            let get_client_config = async || {
                ClientConfig::default().with_auth().await.map_err(|e| {
                    use google_cloud_storage::client::google_cloud_auth::error::Error::HttpError;

                    // retry when we may receive an error from hitting the credentials server too much
                    if let HttpError(reqwest_err) = &e
                        && (reqwest_err.is_request()
                            || reqwest_err.is_timeout()
                            || matches!(reqwest_err.status().map(|s| s.as_u16()), Some(429) | Some(500..599)))
                    {
                        log::warn!("Encountered HTTP error while attempting to fetch Google Cloud Storage client: {reqwest_err}. Retrying...");
                        RetryError::Transient(e)
                    } else {
                        RetryError::Permanent(e)
                    }
                })
            };

            let attempted = backoff
                .retry(get_client_config)
                .await
                .context(UnableToLoadCredentialsSnafu {});

            match attempted {
                Ok(attempt) => attempt,
                Err(err) => {
                    log::warn!("Google Cloud Storage Credentials not provided or found when making client. Reverting to Anonymous mode.\nDetails\n{err}");
                    ClientConfig::default().anonymous()
                }
            }
        };

        if config.project_id.is_some() {
            client_config.project_id.clone_from(&config.project_id);
        }
        client_config.http = Some({
            use reqwest_middleware::ClientBuilder;
            use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
            use retry_policies::Jitter;
            let retry_policy = ExponentialBackoff::builder()
                .base(2)
                .jitter(Jitter::Bounded)
                .retry_bounds(
                    Duration::from_millis(config.retry_initial_backoff_ms),
                    Duration::from_secs(60),
                )
                .build_with_max_retries(config.num_tries);

            let base_client = reqwest_middleware::reqwest::ClientBuilder::default()
                .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
                .read_timeout(Duration::from_millis(config.read_timeout_ms))
                .pool_idle_timeout(Duration::from_secs(60))
                .pool_max_idle_per_host(70)
                .build()
                .context(UnableToCreateClientSnafu)?;

            ClientBuilder::new(base_client)
                // reqwest-retry already comes with a default retry strategy that matches http standards
                // override it only if you need a custom one due to non standard behavior
                .with(
                    RetryTransientMiddleware::new_with_policy(retry_policy)
                        .with_retry_log_level(tracing::Level::DEBUG),
                )
                .build()
        });

        let client = Client::new(client_config);

        let connection_pool_sema = Arc::new(tokio::sync::Semaphore::new(
            (config.max_connections_per_io_thread as usize)
                * get_io_pool_num_threads().expect("Should be running in tokio pool"),
        ));
        Ok(Self {
            client: GCSClientWrapper {
                client,
                connection_pool_sema,
            },
        }
        .into())
    }
}

#[async_trait]
impl ObjectSource for GCSSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        self.client.get(uri, range, io_stats).await
    }

    async fn put(
        &self,
        _uri: &str,
        _data: bytes::Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        todo!("PUTS to GCS are not yet supported! Please file an issue.");
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        self.client.get_size(uri, io_stats).await
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
    ) -> super::Result<LSResult> {
        self.client
            .ls(path, posix, continuation_token, page_size, io_stats)
            .await
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
