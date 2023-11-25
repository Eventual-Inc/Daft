use async_trait::async_trait;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::retry::RetryMode;
use aws_config::timeout::TimeoutConfig;
use aws_smithy_async::rt::sleep::TokioSleep;
use futures::stream::BoxStream;
use reqwest::StatusCode;
use s3::operation::head_object::HeadObjectError;
use s3::operation::list_objects_v2::ListObjectsV2Error;
use tokio::sync::{OwnedSemaphorePermit, SemaphorePermit};

use crate::object_io::{FileMetadata, FileType, LSResult};
use crate::stats::IOStatsRef;
use crate::stream_utils::io_stats_on_bytestream;
use crate::{get_io_pool_num_threads, InvalidArgumentSnafu, SourceType};
use aws_config::SdkConfig;
use aws_credential_types::cache::{ProvideCachedCredentials, SharedCredentialsCache};
use aws_credential_types::provider::error::CredentialsError;
use aws_sig_auth::signer::SigningRequirements;
use common_io_config::S3Config;
use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::config::{Credentials, Region};
use s3::error::{DisplayErrorContext, SdkError};
use s3::operation::get_object::GetObjectError;
use snafu::{ensure, IntoError, ResultExt, Snafu};
use url::{ParseError, Position};

use super::object_io::{GetResult, ObjectSource};
use async_recursion::async_recursion;
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;

use std::collections::HashMap;

use std::ops::Range;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;

const S3_DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
pub(crate) struct S3LikeSource {
    region_to_client_map: tokio::sync::RwLock<HashMap<Region, Arc<s3::Client>>>,
    connection_pool_sema: Arc<tokio::sync::Semaphore>,
    default_region: Region,
    s3_config: S3Config,
    anonymous: bool,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, s3::error::DisplayErrorContext(source)))]
    UnableToOpenFile {
        path: String,
        source: SdkError<GetObjectError, Response>,
    },

    #[snafu(display("Unable to head {}: {}", path, s3::error::DisplayErrorContext(source)))]
    UnableToHeadFile {
        path: String,
        source: SdkError<HeadObjectError, Response>,
    },

    #[snafu(display("Unable to list {}: {}", path, s3::error::DisplayErrorContext(source)))]
    UnableToListObjects {
        path: String,
        source: SdkError<ListObjectsV2Error, Response>,
    },

    #[snafu(display("Unable to query the region for {}: {}", path, source))]
    UnableToQueryRegion {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable missing header: {header} when performing request for: {path}"))]
    MissingHeader { path: String, header: String },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: ByteStreamError,
    },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },

    #[snafu(display("Not Found: \"{}\"", path))]
    NotFound { path: String },

    #[snafu(display("Unable to load Credentials: {}", source))]
    UnableToLoadCredentials { source: CredentialsError },

    #[snafu(display("Unable to create http client. {}", source))]
    UnableToCreateClient { source: reqwest::Error },

    #[snafu(display("Unable to grab semaphore. {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8 { path: String, source: FromUtf8Error },
    #[snafu(display("Unable to create TlsConnector. {source}"))]
    UnableToCreateTlsConnector {
        source: hyper_tls::native_tls::Error,
    },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source } => match source.into_service_error() {
                GetObjectError::NoSuchKey(no_such_key) => super::Error::NotFound {
                    path,
                    source: no_such_key.into(),
                },
                GetObjectError::Unhandled(v) => super::Error::Unhandled {
                    path,
                    msg: DisplayErrorContext(v).to_string(),
                },
                err => super::Error::UnableToOpenFile {
                    path,
                    source: err.into(),
                },
            },
            UnableToHeadFile { path, source } => match source.into_service_error() {
                HeadObjectError::NotFound(no_such_key) => super::Error::NotFound {
                    path,
                    source: no_such_key.into(),
                },
                HeadObjectError::Unhandled(v) => super::Error::Unhandled {
                    path,
                    msg: DisplayErrorContext(v).to_string(),
                },
                err => super::Error::UnableToOpenFile {
                    path,
                    source: err.into(),
                },
            },
            UnableToListObjects { path, source } => match source.into_service_error() {
                ListObjectsV2Error::NoSuchBucket(no_such_key) => super::Error::NotFound {
                    path,
                    source: no_such_key.into(),
                },
                ListObjectsV2Error::Unhandled(v) => super::Error::Unhandled {
                    path,
                    msg: DisplayErrorContext(v).to_string(),
                },
                err => super::Error::UnableToOpenFile {
                    path,
                    source: err.into(),
                },
            },
            InvalidUrl { path, source } => super::Error::InvalidUrl { path, source },
            UnableToReadBytes { path, source } => super::Error::UnableToReadBytes {
                path,
                source: source.into(),
            },
            NotAFile { path } => super::Error::NotAFile { path },
            UnableToLoadCredentials { source } => super::Error::UnableToLoadCredentials {
                store: SourceType::S3,
                source: source.into(),
            },
            UnableToCreateClient { source } => super::Error::UnableToCreateClient {
                store: SourceType::S3,
                source: source.into(),
            },
            NotFound { ref path } => super::Error::NotFound {
                path: path.into(),
                source: error.into(),
            },
            err => super::Error::Generic {
                store: SourceType::S3,
                source: err.into(),
            },
        }
    }
}

/// Helper to parse S3 URLs, returning (scheme, bucket, key)
fn parse_url(uri: &str) -> super::Result<(String, String, String)> {
    let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
    let bucket = match parsed.host_str() {
        Some(s) => Ok(s),
        None => Err(Error::InvalidUrl {
            path: uri.into(),
            source: ParseError::EmptyHost,
        }),
    }?;

    // Use raw `uri` for object key: URI special character escaping might mangle key
    let bucket_scheme_prefix_len = parsed[..Position::AfterHost].len();
    let key = uri[bucket_scheme_prefix_len..].trim_start_matches(S3_DELIMITER);

    Ok((
        parsed.scheme().to_string(),
        bucket.to_string(),
        key.to_string(),
    ))
}

fn handle_https_client_settings(
    builder: aws_sdk_s3::config::Builder,
    config: &S3Config,
) -> super::Result<aws_sdk_s3::config::Builder> {
    let tls_connector = hyper_tls::native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(!config.verify_ssl)
        .danger_accept_invalid_hostnames((!config.verify_ssl) || (!config.check_hostname_ssl))
        .build()
        .context(UnableToCreateTlsConnectorSnafu {})?;
    let mut http_connector = hyper::client::HttpConnector::new();
    http_connector.enforce_http(false);
    let https_connector = hyper_tls::HttpsConnector::<hyper::client::HttpConnector>::from((
        http_connector,
        tls_connector.into(),
    ));
    use aws_smithy_client::http_connector::ConnectorSettings;
    use aws_smithy_client::hyper_ext;
    let smithy_client = hyper_ext::Adapter::builder()
        .connector_settings(
            ConnectorSettings::builder()
                .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
                .read_timeout(Duration::from_millis(config.read_timeout_ms))
                .build(),
        )
        .build(https_connector);
    let builder = builder.http_connector(smithy_client);
    Ok(builder)
}

async fn build_s3_client(
    config: &S3Config,
    credentials_cache: Option<SharedCredentialsCache>,
) -> super::Result<(bool, s3::Client)> {
    const DEFAULT_REGION: Region = Region::from_static("us-east-1");

    let mut anonymous = config.anonymous;

    let conf: SdkConfig = if anonymous {
        aws_config::SdkConfig::builder().build()
    } else {
        aws_config::load_from_env().await
    };
    let builder = aws_sdk_s3::config::Builder::from(&conf);
    let builder = match &config.endpoint_url {
        None => builder,
        Some(endpoint) => builder.endpoint_url(endpoint).force_path_style(true),
    };
    let builder = if let Some(region) = &config.region_name {
        builder.region(Region::new(region.to_owned()))
    } else {
        builder
    };

    ensure!(
        config.num_tries > 0,
        InvalidArgumentSnafu {
            msg: "num_tries must be greater than zero"
        }
    );
    let retry_config = s3::config::retry::RetryConfig::standard()
        .with_max_attempts(config.num_tries)
        .with_initial_backoff(Duration::from_millis(config.retry_initial_backoff_ms));

    let retry_config = if let Some(retry_mode) = &config.retry_mode {
        if retry_mode.trim().eq_ignore_ascii_case("adaptive") {
            retry_config.with_retry_mode(RetryMode::Adaptive)
        } else if retry_mode.trim().eq_ignore_ascii_case("standard") {
            retry_config
        } else {
            return Err(crate::Error::InvalidArgument { msg: format!("Invalid Retry Mode, Daft S3 client currently only supports standard and adaptive, got {}", retry_mode) });
        }
    } else {
        retry_config
    };

    let builder = builder.retry_config(retry_config);

    let builder = handle_https_client_settings(builder, config)?;

    let sleep_impl = Arc::new(TokioSleep::new());
    let builder = builder.sleep_impl(sleep_impl);
    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .read_timeout(Duration::from_millis(config.read_timeout_ms))
        .build();
    let builder = builder.timeout_config(timeout_config);

    let cached_creds = if let Some(credentials_cache) = credentials_cache {
        let creds = credentials_cache.provide_cached_credentials().await;
        creds.ok()
    } else {
        None
    };

    let builder = if let Some(cached_creds) = cached_creds {
        let provider = CredentialsProviderChain::first_try("different_region_cache", cached_creds)
            .or_default_provider()
            .await;
        builder.credentials_provider(provider)
    } else if config.access_key.is_some() && config.key_id.is_some() {
        let creds = Credentials::from_keys(
            config.key_id.clone().unwrap(),
            config.access_key.clone().unwrap(),
            config.session_token.clone(),
        );
        builder.credentials_provider(creds)
    } else if config.access_key.is_some() || config.key_id.is_some() {
        return Err(super::Error::InvalidArgument {
            msg: "Must provide both access_key and key_id when building S3-Like Client".to_string(),
        });
    } else {
        builder
    };

    let builder_copy = builder.clone();
    let s3_conf = builder.build();
    const CRED_TRIES: u64 = 4;
    const JITTER_MS: u64 = 2_500;
    const MAX_BACKOFF_MS: u64 = 20_000;
    const MAX_WAITTIME_MS: u64 = 45_000;
    let check_creds = async || -> super::Result<bool> {
        use rand::Rng;
        use CredentialsError::*;
        let mut attempt = 0;
        let first_attempt_time = std::time::Instant::now();
        loop {
            let creds = s3_conf
                .credentials_cache()
                .provide_cached_credentials()
                .await;
            attempt += 1;
            match creds {
                Ok(_) => return Ok(false),
                Err(err @  ProviderTimedOut(..)) => {
                    let total_time_waited_ms: u64 = first_attempt_time.elapsed().as_millis().try_into().unwrap();
                    if attempt < CRED_TRIES && (total_time_waited_ms < MAX_WAITTIME_MS) {
                        let jitter = rand::thread_rng().gen_range(0..((2<<attempt) * JITTER_MS)) as u64;
                        let jitter = jitter.min(MAX_BACKOFF_MS);
                        log::warn!("S3 Credentials Provider timed out when making client for {}! Attempt {attempt} out of {CRED_TRIES} tries. Trying again in {jitter}ms. {err}", s3_conf.region().unwrap_or(&DEFAULT_REGION));
                        tokio::time::sleep(Duration::from_millis(jitter)).await;
                        continue;
                    } else {
                        Err(err)
                    }
                }
                Err(err @ CredentialsNotLoaded(..)) => {
                    log::warn!("S3 Credentials not provided or found when making client for {}! Reverting to Anonymous mode. {err}", s3_conf.region().unwrap_or(&DEFAULT_REGION));
                    return Ok(true)
                },
                Err(err) => Err(err),
            }.with_context(|_| UnableToLoadCredentialsSnafu {})?;
        }
    };

    if !config.anonymous {
        anonymous = check_creds().await?;
    };

    let s3_conf = if s3_conf.region().is_none() {
        builder_copy.region(DEFAULT_REGION).build()
    } else {
        s3_conf
    };
    Ok((anonymous, s3::Client::from_conf(s3_conf)))
}

async fn build_client(config: &S3Config) -> super::Result<S3LikeSource> {
    let (anonymous, client) = build_s3_client(config, None).await?;
    let mut client_map = HashMap::new();
    let default_region = client.conf().region().unwrap().clone();
    client_map.insert(default_region.clone(), client.into());
    Ok(S3LikeSource {
        region_to_client_map: tokio::sync::RwLock::new(client_map),
        connection_pool_sema: Arc::new(tokio::sync::Semaphore::new(
            (config.max_connections_per_io_thread as usize)
                * get_io_pool_num_threads()
                    .await
                    .expect("Should be running in tokio pool"),
        )),
        s3_config: config.clone(),
        default_region,
        anonymous,
    })
}
const REGION_HEADER: &str = "x-amz-bucket-region";

impl S3LikeSource {
    pub async fn get_client(config: &S3Config) -> super::Result<Arc<S3LikeSource>> {
        Ok(build_client(config).await?.into())
    }

    async fn get_s3_client(&self, region: &Region) -> super::Result<Arc<s3::Client>> {
        {
            if let Some(client) = self.region_to_client_map.read().await.get(region) {
                return Ok(client.clone());
            }
        }

        let mut w_handle = self.region_to_client_map.write().await;

        if let Some(client) = w_handle.get(region) {
            return Ok(client.clone());
        }

        let mut new_config = self.s3_config.clone();
        new_config.region_name = Some(region.to_string());

        let creds_cache = w_handle
            .get(&self.default_region)
            .map(|current_client| current_client.conf().credentials_cache());

        let (_, new_client) = build_s3_client(&new_config, creds_cache).await?;

        if w_handle.get(region).is_none() {
            w_handle.insert(region.clone(), new_client.into());
        }
        Ok(w_handle.get(region).unwrap().clone())
    }

    #[async_recursion]
    async fn _get_impl(
        &self,
        permit: OwnedSemaphorePermit,
        uri: &str,
        range: Option<Range<usize>>,
        region: &Region,
    ) -> super::Result<GetResult> {
        log::debug!("S3 get at {uri}, range: {range:?}, in region: {region}");
        let (_scheme, bucket, key) = parse_url(uri)?;

        if key.is_empty() {
            Err(Error::NotAFile { path: uri.into() }.into())
        } else {
            log::debug!("S3 get parsed uri: {uri} into Bucket: {bucket}, Key: {key}");
            let request = self
                .get_s3_client(region)
                .await?
                .get_object()
                .bucket(bucket)
                .key(key);

            let request = match &range {
                None => request,
                Some(range) => request.range(format!(
                    "bytes={}-{}",
                    range.start,
                    range.end.saturating_sub(1)
                )),
            };

            let response = if self.anonymous {
                request
                    .customize_middleware()
                    .await
                    .unwrap()
                    .map_operation::<Error>(|mut o| {
                        {
                            let mut properties = o.properties_mut();
                            #[allow(unused_mut)]
                            let mut config = properties
                                .get_mut::<::aws_sig_auth::signer::OperationSigningConfig>()
                                .expect("signing config added by make_operation()");

                            config.signing_requirements = SigningRequirements::Disabled;
                        }
                        Ok(o)
                    })
                    .unwrap()
                    .send()
                    .await
            } else {
                request.send().await
            };

            match response {
                Ok(v) => {
                    let body = v.body;
                    let owned_string = uri.to_owned();
                    let stream = body
                        .map_err(move |e| {
                            UnableToReadBytesSnafu {
                                path: owned_string.clone(),
                            }
                            .into_error(e)
                            .into()
                        })
                        .boxed();
                    Ok(GetResult::Stream(
                        stream,
                        Some(v.content_length as usize),
                        Some(permit),
                    ))
                }

                Err(SdkError::ServiceError(err)) => {
                    let bad_response = err.raw().http();
                    match bad_response.status() {
                        StatusCode::MOVED_PERMANENTLY => {
                            let headers = bad_response.headers();
                            let new_region =
                                headers.get(REGION_HEADER).ok_or(Error::MissingHeader {
                                    path: uri.into(),
                                    header: REGION_HEADER.into(),
                                })?;

                            let region_name = String::from_utf8(new_region.as_bytes().to_vec())
                                .with_context(|_| UnableToParseUtf8Snafu::<String> {
                                    path: uri.into(),
                                })?;

                            let new_region = Region::new(region_name);
                            log::debug!("S3 Region of {uri} different than client {:?} vs {:?} Attempting GET in that region with new client", new_region, region);
                            self._get_impl(permit, uri, range, &new_region).await
                        }
                        _ => Err(UnableToOpenFileSnafu { path: uri }
                            .into_error(SdkError::ServiceError(err))
                            .into()),
                    }
                }
                Err(err) => Err(UnableToOpenFileSnafu { path: uri }.into_error(err).into()),
            }
        }
    }

    #[async_recursion]
    async fn _head_impl(
        &self,
        _permit: SemaphorePermit<'async_recursion>,
        uri: &str,
        region: &Region,
    ) -> super::Result<usize> {
        log::debug!("S3 head at {uri} in region: {region}");
        let (_scheme, bucket, key) = parse_url(uri)?;

        if key.is_empty() {
            Err(Error::NotAFile { path: uri.into() }.into())
        } else {
            log::debug!("S3 head parsed uri: {uri} into Bucket: {bucket}, Key: {key}");
            let request = self
                .get_s3_client(region)
                .await?
                .head_object()
                .bucket(bucket)
                .key(key);

            let response = if self.anonymous {
                request
                    .customize_middleware()
                    .await
                    .unwrap()
                    .map_operation::<Error>(|mut o| {
                        {
                            let mut properties = o.properties_mut();
                            #[allow(unused_mut)]
                            let mut config = properties
                                .get_mut::<::aws_sig_auth::signer::OperationSigningConfig>()
                                .expect("signing config added by make_operation()");

                            config.signing_requirements = SigningRequirements::Disabled;
                        }
                        Ok(o)
                    })
                    .unwrap()
                    .send()
                    .await
            } else {
                request.send().await
            };

            match response {
                Ok(v) => Ok(v.content_length() as usize),
                Err(SdkError::ServiceError(err)) => {
                    let bad_response = err.raw().http();
                    match bad_response.status() {
                        StatusCode::MOVED_PERMANENTLY => {
                            let headers = bad_response.headers();
                            let new_region =
                                headers.get(REGION_HEADER).ok_or(Error::MissingHeader {
                                    path: uri.into(),
                                    header: REGION_HEADER.into(),
                                })?;

                            let region_name = String::from_utf8(new_region.as_bytes().to_vec())
                                .with_context(|_| UnableToParseUtf8Snafu::<String> {
                                    path: uri.into(),
                                })?;

                            let new_region = Region::new(region_name);
                            log::debug!("S3 Region of {uri} different than client {:?} vs {:?} Attempting HEAD in that region with new client", new_region, region);
                            self._head_impl(_permit, uri, &new_region).await
                        }
                        _ => Err(UnableToHeadFileSnafu { path: uri }
                            .into_error(SdkError::ServiceError(err))
                            .into()),
                    }
                }
                Err(err) => Err(UnableToHeadFileSnafu { path: uri }.into_error(err).into()),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn _list_impl(
        &self,
        _permit: SemaphorePermit<'async_recursion>,
        scheme: &str,
        bucket: &str,
        key: &str,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        region: &Region,
        page_size: Option<i32>,
    ) -> super::Result<LSResult> {
        log::debug!("S3 list_objects: Bucket: {bucket}, Key: {key}, continuation_token: {continuation_token:?} in region: {region}");
        let request = self
            .get_s3_client(region)
            .await?
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key);
        let request = if let Some(delimiter) = delimiter.as_ref() {
            request.delimiter(delimiter)
        } else {
            request
        };
        let request = if let Some(ref continuation_token) = continuation_token {
            request.continuation_token(continuation_token)
        } else {
            request
        };
        let request = if let Some(page_size) = page_size {
            request.max_keys(page_size)
        } else {
            request
        };
        let response = if self.anonymous {
            request
                .customize_middleware()
                .await
                .unwrap()
                .map_operation::<Error>(|mut o| {
                    {
                        let mut properties = o.properties_mut();
                        #[allow(unused_mut)]
                        let mut config = properties
                            .get_mut::<::aws_sig_auth::signer::OperationSigningConfig>()
                            .expect("signing config added by make_operation()");

                        config.signing_requirements = SigningRequirements::Disabled;
                    }
                    Ok(o)
                })
                .unwrap()
                .send()
                .await
        } else {
            request.send().await
        };
        let uri = &format!("{scheme}://{bucket}/{key}");
        match response {
            Ok(v) => {
                let dirs = v.common_prefixes();
                let files = v.contents();
                let continuation_token = v.next_continuation_token().map(|s| s.to_string());
                let mut total_len = 0;
                if let Some(dirs) = dirs {
                    total_len += dirs.len()
                }
                if let Some(files) = files {
                    total_len += files.len()
                }
                let mut all_files = Vec::with_capacity(total_len);
                if let Some(dirs) = dirs {
                    for d in dirs {
                        let fmeta = FileMetadata {
                            filepath: format!(
                                "{scheme}://{bucket}/{}",
                                d.prefix().unwrap_or_default()
                            ),
                            size: None,
                            filetype: FileType::Directory,
                        };
                        all_files.push(fmeta);
                    }
                }
                if let Some(files) = files {
                    for f in files {
                        let fmeta = FileMetadata {
                            filepath: format!(
                                "{scheme}://{bucket}/{}",
                                f.key().unwrap_or_default()
                            ),
                            size: Some(f.size() as u64),
                            filetype: FileType::File,
                        };
                        all_files.push(fmeta);
                    }
                }
                Ok(LSResult {
                    files: all_files,
                    continuation_token,
                })
            }
            Err(SdkError::ServiceError(err)) => {
                let bad_response = err.raw().http();
                match bad_response.status() {
                    StatusCode::MOVED_PERMANENTLY => {
                        let headers = bad_response.headers();
                        let new_region =
                            headers.get(REGION_HEADER).ok_or(Error::MissingHeader {
                                path: uri.into(),
                                header: REGION_HEADER.into(),
                            })?;

                        let region_name = String::from_utf8(new_region.as_bytes().to_vec())
                            .with_context(|_| UnableToParseUtf8Snafu::<String> {
                                path: uri.into(),
                            })?;

                        let new_region = Region::new(region_name);
                        log::debug!("S3 Region of {uri} different than client {:?} vs {:?} Attempting List in that region with new client", new_region, region);
                        self._list_impl(
                            _permit,
                            scheme,
                            bucket,
                            key,
                            delimiter,
                            continuation_token.clone(),
                            &new_region,
                            page_size,
                        )
                        .await
                    }
                    _ => Err(UnableToListObjectsSnafu { path: uri }
                        .into_error(SdkError::ServiceError(err))
                        .into()),
                }
            }
            Err(err) => Err(UnableToListObjectsSnafu { path: uri }
                .into_error(err)
                .into()),
        }
    }
}

#[async_trait]
impl ObjectSource for S3LikeSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        let get_result = self
            ._get_impl(permit, uri, range, &self.default_region)
            .await?;

        if io_stats.is_some() {
            if let GetResult::Stream(stream, num_bytes, permit) = get_result {
                if let Some(is) = io_stats.as_ref() {
                    is.mark_get_requests(1)
                }
                Ok(GetResult::Stream(
                    io_stats_on_bytestream(stream, io_stats),
                    num_bytes,
                    permit,
                ))
            } else {
                panic!("This should always be a stream");
            }
        } else {
            Ok(get_result)
        }
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let permit = self
            .connection_pool_sema
            .acquire()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        let head_result = self._head_impl(permit, uri, &self.default_region).await?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1)
        }
        Ok(head_result)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
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
        let (scheme, bucket, key) = parse_url(path)?;

        if posix {
            // Perform a directory-based list of entries in the next level
            // assume its a directory first
            let key = if key.is_empty() {
                "".to_string()
            } else {
                format!("{}{S3_DELIMITER}", key.trim_end_matches(S3_DELIMITER))
            };
            let lsr = {
                let permit = self
                    .connection_pool_sema
                    .acquire()
                    .await
                    .context(UnableToGrabSemaphoreSnafu)?;

                self._list_impl(
                    permit,
                    scheme.as_str(),
                    bucket.as_str(),
                    &key,
                    Some(S3_DELIMITER.into()),
                    continuation_token.map(String::from),
                    &self.default_region,
                    page_size,
                )
                .await?
            };
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1)
            }

            if lsr.files.is_empty() && key.contains(S3_DELIMITER) {
                let permit = self
                    .connection_pool_sema
                    .acquire()
                    .await
                    .context(UnableToGrabSemaphoreSnafu)?;
                // Might be a File
                let key = key.trim_end_matches(S3_DELIMITER);
                let mut lsr = self
                    ._list_impl(
                        permit,
                        scheme.as_str(),
                        bucket.as_str(),
                        key,
                        Some(S3_DELIMITER.into()),
                        continuation_token.map(String::from),
                        &self.default_region,
                        page_size,
                    )
                    .await?;
                if let Some(is) = io_stats.as_ref() {
                    is.mark_list_requests(1)
                }
                let target_path = format!("{scheme}://{bucket}/{key}");
                lsr.files.retain(|f| f.filepath == target_path);

                if lsr.files.is_empty() {
                    // Isnt a file or a directory
                    return Err(Error::NotFound { path: path.into() }.into());
                }
                Ok(lsr)
            } else {
                Ok(lsr)
            }
        } else {
            // Perform a prefix-based list of all entries with this prefix
            let lsr = {
                let permit = self
                    .connection_pool_sema
                    .acquire()
                    .await
                    .context(UnableToGrabSemaphoreSnafu)?;

                self._list_impl(
                    permit,
                    scheme.as_str(),
                    bucket.as_str(),
                    key.as_str(),
                    None, // triggers prefix-based list
                    continuation_token.map(String::from),
                    &self.default_region,
                    page_size,
                )
                .await?
            };
            if let Some(is) = io_stats.as_ref() {
                is.mark_list_requests(1)
            }

            Ok(lsr)
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::object_io::ObjectSource;
    use crate::Result;
    use crate::S3LikeSource;
    use common_io_config::S3Config;

    #[tokio::test]
    async fn test_full_get_from_s3() -> Result<()> {
        let parquet_file_path = "s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let config = S3Config {
            anonymous: true,
            ..Default::default()
        };
        let client = S3LikeSource::get_client(&config).await?;
        let parquet_file = client.get(parquet_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);

        let first_bytes = client
            .get_range(parquet_file_path, 0..10, None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

        let first_bytes = client
            .get_range(parquet_file_path, 10..100, None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

        let last_bytes = client
            .get_range(
                parquet_file_path,
                (all_bytes.len() - 10)..(all_bytes.len() + 10),
                None,
            )
            .await?
            .bytes()
            .await?;
        assert_eq!(last_bytes.len(), 10);
        assert_eq!(last_bytes.as_ref(), &all_bytes[(all_bytes.len() - 10)..]);

        let size_from_get_size = client.get_size(parquet_file_path, None).await?;
        assert_eq!(size_from_get_size, all_bytes.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_full_ls_from_s3() -> Result<()> {
        let file_path = "s3://daft-public-data/test_fixtures/parquet/";

        let config = S3Config {
            anonymous: true,
            ..Default::default()
        };
        let client = S3LikeSource::get_client(&config).await?;

        client.ls(file_path, true, None, None, None).await?;

        Ok(())
    }
}
