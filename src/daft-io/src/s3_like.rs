use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    io::Write,
    num::{NonZeroI32, NonZeroUsize},
    ops::Range,
    string::FromUtf8Error,
    sync::Arc,
    time::Duration,
};

use async_recursion::async_recursion;
use async_trait::async_trait;
use aws_config::{
    meta::credentials::CredentialsProviderChain, retry::RetryMode, timeout::TimeoutConfig,
    SdkConfig,
};
use aws_credential_types::{
    cache::{CredentialsCache, ProvideCachedCredentials, SharedCredentialsCache},
    provider::error::CredentialsError,
};
use aws_sdk_s3::{
    self as s3,
    error::ProvideErrorMetadata,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError,
        put_object::PutObjectError,
        upload_part::{UploadPartError, UploadPartOutput},
    },
    primitives::ByteStreamError,
};
use aws_sig_auth::signer::SigningRequirements;
use aws_smithy_async::rt::sleep::TokioSleep;
use common_io_config::S3Config;
use common_runtime::get_io_pool_num_threads;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use reqwest::StatusCode;
use s3::{
    client::customize::Response,
    config::{Credentials, Region},
    error::{DisplayErrorContext, SdkError},
    operation::{
        get_object::GetObjectError, head_object::HeadObjectError,
        list_objects_v2::ListObjectsV2Error,
    },
};
use snafu::{ensure, IntoError, OptionExt, ResultExt, Snafu};
use tokio::{
    sync::{mpsc::Sender, OwnedSemaphorePermit, SemaphorePermit},
    task::JoinSet,
};
use url::{ParseError, Position};

use super::object_io::{GetResult, ObjectSource};
use crate::{
    object_io::{FileMetadata, FileType, LSResult},
    retry::{ExponentialBackoff, RetryError},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
    Error::InvalidArgument,
    FileFormat, InvalidArgumentSnafu, SourceType,
};

const S3_DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;

#[derive(Debug)]
pub struct S3LikeSource {
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

    #[snafu(display(
        "Unable to put file to {}: {}",
        path,
        s3::error::DisplayErrorContext(source)
    ))]
    UnableToPutFile {
        path: String,
        source: SdkError<PutObjectError, Response>,
    },

    #[snafu(display(
        "Unable to do create multipart upload to {}/{}: {}",
        bucket,
        key,
        s3::error::DisplayErrorContext(source)
    ))]
    UnableToCreateMultipartUpload {
        bucket: String,
        key: String,
        source: SdkError<CreateMultipartUploadError, Response>,
    },

    #[snafu(display(
        "Unable to upload part {} for {}/{} with upload_id {}: {}",
        part,
        bucket,
        key,
        upload_id,
        s3::error::DisplayErrorContext(source)
    ))]
    UnableToUploadPart {
        bucket: String,
        key: String,
        upload_id: String,
        part: NonZeroI32,
        source: SdkError<UploadPartError, Response>,
    },

    #[snafu(display(
        "Unable to complete multipart upload to {}/{}: {}",
        bucket,
        key,
        s3::error::DisplayErrorContext(source)
    ))]
    UnableToCompleteMultipartUpload {
        bucket: String,
        key: String,
        source: SdkError<CompleteMultipartUploadError, Response>,
    },

    #[snafu(display(
        "Expected multi-part upload ID in CreateMultipartUpload response for {bucket}/{key}",
    ))]
    MissingUploadIdForMultipartUpload { bucket: String, key: String },

    #[snafu(display(
        "Expected ETag in UploadPart response for bucket: {}, key: {}, upload_id: {}, part: {}.",
        bucket,
        key,
        upload_id,
        part
    ))]
    MissingEtagForMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
        part: NonZeroI32,
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

    #[snafu(display("Uploads cannot be anonymous. Please disable anonymous S3 access."))]
    UploadsCannotBeAnonymous {},
}

/// List of AWS error codes that are due to throttling
/// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
const THROTTLING_ERRORS: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
];

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::{
            InvalidUrl, NotAFile, NotFound, UnableToHeadFile, UnableToListObjects,
            UnableToLoadCredentials, UnableToOpenFile, UnableToReadBytes,
        };

        fn classify_unhandled_error<
            E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
        >(
            path: String,
            err: E,
        ) -> super::Error {
            match err.code() {
                Some("InternalError") => super::Error::MiscTransient {
                    path,
                    source: err.into(),
                },
                Some(code) if THROTTLING_ERRORS.contains(&code) => super::Error::Throttled {
                    path,
                    source: err.into(),
                },
                _ => super::Error::Unhandled {
                    path,
                    msg: DisplayErrorContext(err).to_string(),
                },
            }
        }

        match error {
            UnableToOpenFile { path, source } => match source {
                SdkError::TimeoutError(_) => Self::ReadTimeout {
                    path,
                    source: source.into(),
                },
                SdkError::DispatchFailure(ref dispatch) => {
                    if dispatch.is_timeout() {
                        Self::ConnectTimeout {
                            path,
                            source: source.into(),
                        }
                    } else if dispatch.is_io() {
                        Self::SocketError {
                            path,
                            source: source.into(),
                        }
                    } else {
                        // who knows what happened here during dispatch, let's just tell the user it's transient
                        Self::MiscTransient {
                            path,
                            source: source.into(),
                        }
                    }
                }

                _ => match source.into_service_error() {
                    GetObjectError::NoSuchKey(no_such_key) => Self::NotFound {
                        path,
                        source: no_such_key.into(),
                    },
                    err => classify_unhandled_error(path, err),
                },
            },
            UnableToHeadFile { path, source } => match source {
                SdkError::TimeoutError(_) => Self::ReadTimeout {
                    path,
                    source: source.into(),
                },
                SdkError::DispatchFailure(ref dispatch) => {
                    if dispatch.is_timeout() {
                        Self::ConnectTimeout {
                            path,
                            source: source.into(),
                        }
                    } else if dispatch.is_io() {
                        Self::SocketError {
                            path,
                            source: source.into(),
                        }
                    } else {
                        // who knows what happened here during dispatch, let's just tell the user it's transient
                        Self::MiscTransient {
                            path,
                            source: source.into(),
                        }
                    }
                }
                _ => match source.into_service_error() {
                    HeadObjectError::NotFound(no_such_key) => Self::NotFound {
                        path,
                        source: no_such_key.into(),
                    },
                    err => classify_unhandled_error(path, err),
                },
            },
            UnableToListObjects { path, source } => match source {
                SdkError::TimeoutError(_) => Self::ReadTimeout {
                    path,
                    source: source.into(),
                },
                SdkError::DispatchFailure(ref dispatch) => {
                    if dispatch.is_timeout() {
                        Self::ConnectTimeout {
                            path,
                            source: source.into(),
                        }
                    } else if dispatch.is_io() {
                        Self::SocketError {
                            path,
                            source: source.into(),
                        }
                    } else {
                        // who knows what happened here during dispatch, let's just tell the user it's transient
                        Self::MiscTransient {
                            path,
                            source: source.into(),
                        }
                    }
                }
                _ => match source.into_service_error() {
                    ListObjectsV2Error::NoSuchBucket(no_such_key) => Self::NotFound {
                        path,
                        source: no_such_key.into(),
                    },
                    err => classify_unhandled_error(path, err),
                },
            },
            InvalidUrl { path, source } => Self::InvalidUrl { path, source },
            UnableToReadBytes { path, source } => {
                use std::error::Error;
                let io_error = if let Some(source) = source.source() {
                    // if we have a source, lets extract out the error as a string rather than rely on the aws-sdk fmt.
                    let source_as_string = source.to_string();
                    std::io::Error::other(source_as_string)
                } else {
                    std::io::Error::other(source)
                };
                Self::UnableToReadBytes {
                    path,
                    source: io_error,
                }
            }
            NotAFile { path } => Self::NotAFile { path },
            UnableToLoadCredentials { source } => Self::UnableToLoadCredentials {
                store: SourceType::S3,
                source: source.into(),
            },
            NotFound { ref path } => Self::NotFound {
                path: path.into(),
                source: error.into(),
            },
            err => Self::Generic {
                store: SourceType::S3,
                source: err.into(),
            },
        }
    }
}

/// Retrieves an S3Config from the environment by leveraging the AWS SDK's credentials chain
pub async fn s3_config_from_env() -> super::Result<S3Config> {
    let default_s3_config = S3Config::default();
    let (anonymous, s3_conf) = build_s3_conf(&default_s3_config, None).await?;
    let creds = s3_conf
        .credentials_cache()
        .provide_cached_credentials()
        .await
        .with_context(|_| UnableToLoadCredentialsSnafu {})?;
    let key_id = Some(creds.access_key_id().to_string());
    let access_key = Some(creds.secret_access_key().to_string().into());
    let session_token = creds.session_token().map(|t| t.to_string().into());
    let region_name = s3_conf.region().map(std::string::ToString::to_string);
    Ok(S3Config {
        // Do not perform auto-discovery of endpoint_url. This is possible, but requires quite a bit
        // of work that our current implementation of `build_s3_conf` does not yet do. See smithy-rs code:
        // https://github.com/smithy-lang/smithy-rs/blob/94ecd38c2518583042796b2b45c37947237e31dd/aws/rust-runtime/aws-config/src/lib.rs#L824-L849
        endpoint_url: None,
        region_name,
        key_id,
        session_token,
        access_key,
        anonymous,
        ..default_s3_config
    })
}

/// Helper to parse S3 URLs, returning (scheme, bucket, key)
pub fn parse_s3_url(uri: &str) -> super::Result<(String, String, String)> {
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
    use aws_smithy_client::{http_connector::ConnectorSettings, hyper_ext};
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

async fn build_s3_conf(
    config: &S3Config,
    credentials_cache: Option<SharedCredentialsCache>,
) -> super::Result<(bool, s3::Config)> {
    const DEFAULT_REGION: Region = Region::from_static("us-east-1");

    let mut anonymous = config.anonymous;

    let cached_creds = if let Some(credentials_cache) = credentials_cache {
        let creds = credentials_cache.provide_cached_credentials().await;
        creds.ok()
    } else {
        None
    };

    let provider = if let Some(cached_creds) = cached_creds {
        let provider = CredentialsProviderChain::first_try("different_region_cache", cached_creds)
            .or_default_provider()
            .await;
        Some(aws_credential_types::provider::SharedCredentialsProvider::new(provider))
    } else if let Some(provider) = &config.credentials_provider {
        Some(aws_credential_types::provider::SharedCredentialsProvider::new(provider.clone()))
    } else if config.access_key.is_some() && config.key_id.is_some() {
        let creds = Credentials::from_keys(
            config.key_id.clone().unwrap(),
            config
                .access_key
                .as_ref()
                .map(|s| s.as_string().clone())
                .unwrap(),
            config.session_token.as_ref().map(|s| s.as_string().clone()),
        );
        Some(aws_credential_types::provider::SharedCredentialsProvider::new(creds))
    } else if config.access_key.is_some() || config.key_id.is_some() {
        return Err(super::Error::InvalidArgument {
            msg: "Must provide both access_key and key_id when building S3-Like Client".to_string(),
        });
    } else {
        None
    };

    let conf: SdkConfig = if anonymous {
        let mut builder = aws_config::SdkConfig::builder();
        builder.set_credentials_provider(provider);
        builder.build()
    } else {
        let mut loader = aws_config::from_env();
        if let Some(profile_name) = &config.profile_name {
            loader = loader.profile_name(profile_name);
        }

        // Set region now to avoid imds
        if let Some(region) = &config.region_name {
            loader = loader.region(Region::new(region.to_owned()));
        }

        // Set creds now to avoid imds
        if let Some(provider) = provider {
            loader = loader.credentials_provider(provider);
        }

        if let Some(buffer_time) = &config.buffer_time {
            loader = loader.credentials_cache(
                CredentialsCache::lazy_builder()
                    .buffer_time(Duration::from_secs(*buffer_time))
                    .into_credentials_cache(),
            );
        }

        loader.load().await
    };

    let builder = aws_sdk_s3::config::Builder::from(&conf);
    let builder = match &config.endpoint_url {
        None => builder,
        Some(endpoint) => builder.endpoint_url(endpoint),
    };
    let builder = if config.endpoint_url.is_some() && !config.force_virtual_addressing {
        builder.force_path_style(true)
    } else {
        builder.force_path_style(false)
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
            return Err(crate::Error::InvalidArgument { msg: format!("Invalid Retry Mode, Daft S3 client currently only supports standard and adaptive, got {retry_mode}") });
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

    let builder_copy = builder.clone();
    let s3_conf = builder.build();

    let backoff = ExponentialBackoff {
        max_waittime_ms: Some(45_000),
        ..Default::default()
    };

    let check_creds = async || {
        use CredentialsError::{CredentialsNotLoaded, ProviderTimedOut};

        let creds = s3_conf
            .credentials_cache()
            .provide_cached_credentials()
            .await;

        match creds {
            Ok(_) => Ok(false),
            Err(err @ ProviderTimedOut(..)) => {
                log::warn!(
                    "S3 Credentials Provider timed out when making client for {}! Retrying. {err}",
                    s3_conf.region().unwrap_or(&DEFAULT_REGION)
                );
                Err(RetryError::Transient(err))
            }
            Err(err @ CredentialsNotLoaded(..)) => {
                log::warn!("S3 Credentials not provided or found when making client for {}! Reverting to Anonymous mode. {err}", s3_conf.region().unwrap_or(&DEFAULT_REGION));
                Ok(true)
            }
            Err(err) => Err(RetryError::Permanent(err)),
        }
    };

    if !config.anonymous {
        anonymous = backoff
            .retry(check_creds)
            .await
            .with_context(|_| UnableToLoadCredentialsSnafu {})?;
    }

    let s3_conf = if s3_conf.region().is_none() {
        builder_copy.region(DEFAULT_REGION).build()
    } else {
        s3_conf
    };

    Ok((anonymous, s3_conf))
}

async fn build_s3_client(
    config: &S3Config,
    credentials_cache: Option<SharedCredentialsCache>,
) -> super::Result<(bool, s3::Client)> {
    let (anonymous, s3_conf) = build_s3_conf(config, credentials_cache).await?;
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
                * get_io_pool_num_threads().expect("Should be running in tokio pool"),
        )),
        s3_config: config.clone(),
        default_region,
        anonymous,
    })
}
const REGION_HEADER: &str = "x-amz-bucket-region";

impl S3LikeSource {
    pub async fn get_client(config: &S3Config) -> super::Result<Arc<Self>> {
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
    async fn get_impl(
        &self,
        permit: OwnedSemaphorePermit,
        uri: &str,
        range: Option<Range<usize>>,
        region: &Region,
    ) -> super::Result<GetResult> {
        log::debug!("S3 get at {uri}, range: {range:?}, in region: {region}");
        let (_scheme, bucket, key) = parse_s3_url(uri)?;

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

            let request = if self.s3_config.requester_pays {
                request.request_payer(s3::types::RequestPayer::Requester)
            } else {
                request
            };

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
                        None,
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
                            self.get_impl(permit, uri, range, &new_region).await
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
    async fn head_impl(
        &self,
        permit: SemaphorePermit<'async_recursion>,
        uri: &str,
        region: &Region,
    ) -> super::Result<usize> {
        log::debug!("S3 head at {uri} in region: {region}");
        let (_scheme, bucket, key) = parse_s3_url(uri)?;

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

            let request = if self.s3_config.requester_pays {
                request.request_payer(s3::types::RequestPayer::Requester)
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
                            self.head_impl(permit, uri, &new_region).await
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
    async fn list_impl(
        &self,
        permit: SemaphorePermit<'async_recursion>,
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
        let request = if self.s3_config.requester_pays {
            request.request_payer(s3::types::RequestPayer::Requester)
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
                let continuation_token = v
                    .next_continuation_token()
                    .map(std::string::ToString::to_string);
                let mut total_len = 0;
                if let Some(dirs) = dirs {
                    total_len += dirs.len();
                }
                if let Some(files) = files {
                    total_len += files.len();
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
                        self.list_impl(
                            permit,
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

    #[async_recursion]
    async fn put_impl(
        &self,
        _permit: OwnedSemaphorePermit,
        uri: &str,
        data: bytes::Bytes,
        region: &Region,
    ) -> super::Result<()> {
        log::debug!(
            "S3 put at {uri}, num_bytes: {}, in region: {region}",
            data.len()
        );
        let (_scheme, bucket, key) = parse_s3_url(uri)?;

        if key.is_empty() {
            Err(Error::NotAFile { path: uri.into() }.into())
        } else {
            log::debug!("S3 put parsed uri: {uri} into Bucket: {bucket}, Key: {key}");
            let request = self
                .get_s3_client(region)
                .await?
                .put_object()
                .body(data.into())
                .bucket(bucket)
                .key(key);

            let request = if self.s3_config.requester_pays {
                request.request_payer(s3::types::RequestPayer::Requester)
            } else {
                request
            };

            let response = if self.anonymous {
                return Err(Error::UploadsCannotBeAnonymous {}.into());
            } else {
                request.send().await
            };

            match response {
                Ok(_) => Ok(()),
                Err(err) => Err(UnableToPutFileSnafu { path: uri }.into_error(err).into()),
            }
        }
    }

    /// Initiates a multipart upload and returns the upload ID.
    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
    ) -> super::Result<Cow<'static, str>> {
        if self.anonymous {
            return Err(Error::UploadsCannotBeAnonymous {}.into());
        }

        let region = &self.default_region;

        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let request_payer = if self.s3_config.requester_pays {
            Some(s3::types::RequestPayer::Requester)
        } else {
            None
        };

        let client = self.get_s3_client(region).await?;

        let create_multipart_upload_response = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .set_request_payer(request_payer.clone())
            .send()
            .await
            .context(UnableToCreateMultipartUploadSnafu { bucket, key })?;

        let upload_id = create_multipart_upload_response
            .upload_id()
            .ok_or_else(|| Error::MissingUploadIdForMultipartUpload {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            })?;

        log::debug!("S3 create multipart upload-id: {upload_id}");

        Ok(upload_id.to_owned().into())
    }

    /// Completes a multipart upload by providing the upload ID and a list of completed parts.
    pub async fn complete_multipart_upload(
        &self,
        key: Cow<'static, str>,
        bucket: Cow<'static, str>,
        upload_id: Cow<'static, str>,
        completed_parts: Vec<CompletedPart>,
    ) -> super::Result<()> {
        if self.anonymous {
            return Err(Error::UploadsCannotBeAnonymous {}.into());
        }

        let region = &self.default_region;

        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let client = self.get_s3_client(region).await?;

        let completed_parts = completed_parts
            .into_iter()
            .map(|part| {
                s3::types::CompletedPart::builder()
                    .part_number(part.part_number.get())
                    .e_tag(part.etag.clone())
                    .build()
            })
            .collect::<Vec<_>>();

        let completed_multipart_upload = s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        client
            .complete_multipart_upload()
            .multipart_upload(completed_multipart_upload)
            .bucket(bucket.clone())
            .key(key.clone())
            .upload_id(upload_id.clone())
            .send()
            .await
            .context(UnableToCompleteMultipartUploadSnafu { bucket, key })?;

        log::debug!("S3 complete multipart upload completed. upload_id :{upload_id}");

        Ok(())
    }

    /// Upload a single part to an existing multipart upload.
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: NonZeroI32,
        data: bytes::Bytes,
    ) -> super::Result<UploadPartOutput> {
        if self.anonymous {
            return Err(Error::UploadsCannotBeAnonymous {}.into());
        }

        let region = &self.default_region;

        let _permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let request_payer = if self.s3_config.requester_pays {
            Some(s3::types::RequestPayer::Requester)
        } else {
            None
        };

        let client = self.get_s3_client(region).await?;

        let output = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number.get())
            .body(data.into())
            .set_request_payer(request_payer)
            .send()
            .await
            .context(UnableToUploadPartSnafu {
                bucket,
                key,
                upload_id,
                part: part_number,
            })?;

        Ok(output)
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
            .get_impl(permit, uri, range, &self.default_region)
            .await?;

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

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        let data_len = data.len();
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        self.put_impl(permit, uri, data, &self.default_region)
            .await?;

        if let Some(io_stats) = io_stats {
            io_stats.as_ref().mark_put_requests(1);
            io_stats.as_ref().mark_bytes_uploaded(data_len);
        }

        Ok(())
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let permit = self
            .connection_pool_sema
            .acquire()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        let head_result = self.head_impl(permit, uri, &self.default_region).await?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
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
        let (scheme, bucket, key) = parse_s3_url(path)?;

        if posix {
            // Perform a directory-based list of entries in the next level
            // assume its a directory first
            let key = if key.is_empty() {
                String::new()
            } else {
                format!("{}{S3_DELIMITER}", key.trim_end_matches(S3_DELIMITER))
            };
            let lsr = {
                let permit = self
                    .connection_pool_sema
                    .acquire()
                    .await
                    .context(UnableToGrabSemaphoreSnafu)?;

                self.list_impl(
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
                is.mark_list_requests(1);
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
                    .list_impl(
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
                    is.mark_list_requests(1);
                }
                let target_path = format!("{scheme}://{bucket}/{key}");
                lsr.files.retain(|f| f.filepath == target_path);

                if lsr.files.is_empty() {
                    // Isn't a file or a directory
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

                self.list_impl(
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
                is.mark_list_requests(1);
            }

            Ok(lsr)
        }
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// S3MultipartWriter is responsible for managing multipart uploads to S3.
///
/// It handles the creation of the multipart upload, writing individual parts to S3 and also
/// completing the multipart upload once all parts have been uploaded.
///
/// It uses a semaphore to limit upload concurrency (and therefore memory utilization associated
/// with the part data).
#[derive(Debug)]
pub struct S3MultipartWriter {
    /// The URI of the S3 object to write to.
    uri: Cow<'static, str>,

    /// The bucket and key of the S3 object to write to.
    bucket: Cow<'static, str>,

    /// The key of the S3 object to write to.
    key: Cow<'static, str>,

    /// The upload ID of the S3 multipart upload. This is used to identify the multipart upload
    /// to S3.
    upload_id: Cow<'static, str>,

    /// Handles for the parts being uploaded.
    in_progress_uploads: JoinSet<super::Result<CompletedPart>>,

    /// Stores the next part number for multipart upload. See [`generate_part_number`] for a
    /// convenience method to generate the next part number.
    next_part_number: NonZeroI32,

    /// The S3 client used to perform the multipart upload operations.
    s3_client: Arc<S3LikeSource>,

    /// Semaphore to limit the number of concurrent in-flight uploads.
    in_flight_upload_permits: Arc<tokio::sync::Semaphore>,
}

/// Represents a completed part of a multipart upload to S3.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    part_number: NonZeroI32,
    etag: Cow<'static, str>,
}

impl S3MultipartWriter {
    const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024; // 5 Mebibytes
    const MAXIMUM_PART_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5 Gibibytes
    const MAX_PART_COUNT: i32 = 10000; // Max parts in a multipart upload

    /// Ensure that the part size is within the valid range for S3 multipart uploads.
    /// This function checks that the part size is at least 5 MiB and at most 5 GiB.
    fn validate_part_size(part_size: NonZeroUsize) -> super::Result<()> {
        if part_size.get() > Self::MAXIMUM_PART_SIZE {
            return Err(InvalidArgument {
                msg: format!(
                    "Part size must be less than or equal to {} bytes",
                    Self::MAXIMUM_PART_SIZE
                ),
            });
        }
        if part_size.get() < Self::MINIMUM_PART_SIZE {
            return Err(InvalidArgument {
                msg: format!(
                    "Part size must be greater than or equal to {} bytes",
                    Self::MINIMUM_PART_SIZE
                ),
            });
        }
        Ok(())
    }

    /// Creates a new S3MultipartWriter for the specified URI, part size, and maximum concurrent uploads.
    ///
    /// This kicks off the multipart upload process by creating a new multipart upload on S3.
    /// The returned S3MultipartWriter can then be used to write parts to the upload. After all parts
    /// are written, `shutdown()` must be called to finalize the upload.
    pub async fn create(
        uri: impl Into<String>,
        part_size: NonZeroUsize,
        max_concurrent_uploads: NonZeroUsize,
        s3_client: Arc<S3LikeSource>,
    ) -> super::Result<Self> {
        let uri = uri.into();
        let (_scheme, bucket, key) = parse_s3_url(&uri)?;

        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.clone() }.into());
        }

        Self::validate_part_size(part_size)?;

        log::debug!("S3 multipart upload requested: {uri}, part_size: {part_size}");

        let upload_id = s3_client.create_multipart_upload(&bucket, &key).await?;

        log::debug!(
            "S3 multipart upload has been assigned an upload_id: {uri}, upload_id: {upload_id}"
        );

        Ok(Self {
            uri: uri.into(),
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            s3_client,
            next_part_number: unsafe { NonZeroI32::new_unchecked(1) },
            in_progress_uploads: JoinSet::new(),
            in_flight_upload_permits: Arc::new(tokio::sync::Semaphore::new(
                max_concurrent_uploads.get(),
            )),
        })
    }
}

impl S3MultipartWriter {
    /// Generates the next part number for the multipart upload.
    ///
    /// Panics if the next part number exceeds the maximum part count of 10,000.
    fn generate_part_number(&mut self) -> NonZeroI32 {
        let part_number = self.next_part_number;
        self.next_part_number = NonZeroI32::new(part_number.get() + 1).unwrap();
        assert!(
            self.next_part_number.get() <= Self::MAX_PART_COUNT,
            "Maximum part count exceeded"
        );
        part_number
    }

    /// Writes a chunk of data to the S3 multipart upload.
    ///
    /// The part size is expected to be the same as the one specified during the creation of the
    /// S3MultipartWriter. If the part size is different, it will panic.
    ///
    /// A new part number is generated for the part and a new task is spawned to upload the part
    /// in the background.
    pub async fn write_part(&mut self, chunk: bytes::Bytes) -> super::Result<()> {
        // Create an async task to upload the part.
        let data_len = chunk.len();

        let next_part_number = self.generate_part_number();
        let upload_id = self.upload_id.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let s3_client = self.s3_client.clone();

        log::debug!(
            "S3 multipart upload part requested: {next_part_number}, size: {data_len} bytes"
        );
        let upload_permit = self.in_flight_upload_permits.clone().acquire_owned().await;
        log::debug!(
            "S3 multipart upload part permit acquired: {next_part_number}, size: {data_len} bytes"
        );

        let upload_future = async move {
            let output = s3_client
                .upload_part(&bucket, &key, &upload_id, next_part_number, chunk)
                .await?;

            drop(upload_permit);

            log::debug!(
                "S3 upload part has been completed: {next_part_number}, size: {data_len} bytes"
            );

            let etag = output.e_tag().map(|etag| etag.to_string().into()).context(
                MissingEtagForMultipartUploadSnafu {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    upload_id: upload_id.to_string(),
                    part: next_part_number,
                },
            )?;

            Ok(CompletedPart {
                part_number: next_part_number,
                etag,
            })
        };

        // Spawn the upload task and add it to the in-progress uploads.
        self.in_progress_uploads.spawn(upload_future);
        Ok(())
    }

    pub async fn shutdown(&mut self) -> super::Result<()> {
        // Wait for all in-progress uploads to complete.
        let mut completed_parts = vec![];

        while let Some(upload) = self.in_progress_uploads.join_next().await {
            match upload {
                Ok(Ok(part)) => completed_parts.push(part),
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(super::Error::JoinError { source: err }),
            }
        }

        log::debug!(
            "Finalizing multipart upload with {} parts.",
            completed_parts.len()
        );

        // Ensure that completed parts are sorted by in ascending order by part number - else S3
        // will reject the completion request.
        completed_parts.sort_by_key(|part| part.part_number);

        // Complete the multipart upload with the completed parts.
        self.s3_client
            .complete_multipart_upload(
                self.key.clone(),
                self.bucket.clone(),
                self.upload_id.clone(),
                completed_parts
                    .into_iter()
                    .map(|part| part.into())
                    .collect(),
            )
            .await?;

        log::debug!("S3 multipart upload completed: {}", self.uri);
        Ok(())
    }
}

pub struct S3PartBuffer {
    buffer: Vec<u8>,
    part_size: NonZeroUsize,
    tx: Option<Sender<bytes::Bytes>>,
}

impl S3PartBuffer {
    pub fn new(part_size: NonZeroUsize, tx: Sender<bytes::Bytes>) -> Self {
        Self {
            buffer: Vec::with_capacity(part_size.get()),
            part_size,
            tx: Some(tx),
        }
    }

    pub fn shutdown(&mut self) -> std::io::Result<()> {
        log::debug!("Shutting down S3 parts buffer.");

        if !self.buffer.is_empty() {
            log::debug!(
                "S3 parts buffer has {} bytes remaining to send.",
                self.buffer.len()
            );

            // If there is any remaining data in the buffer, send it as a final part
            let old_buffer =
                std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_size.get()));
            let new_part = bytes::Bytes::from(old_buffer);

            if let Some(tx) = &self.tx {
                // Attempt to send the final part
                tx.blocking_send(new_part)
                    .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to send final part for multi-part upload. Has the receiver been dropped?"))?;
            } else {
                panic!("It seems that the S3PartBuffer has been shutdown already, but we still have data to send. This is a bug in the code.");
            }
        }
        self.tx.take();
        Ok(())
    }
}

impl Write for S3PartBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = buf;
        let mut total_written_bytes = 0;

        while !remaining.is_empty() {
            // Write the buffer until its full, or we run out of data
            let available_space = self.part_size.get() - self.buffer.len();
            let writable_bytes = remaining.len().min(available_space);

            self.buffer.extend_from_slice(&remaining[..writable_bytes]);
            total_written_bytes += writable_bytes;
            remaining = &remaining[writable_bytes..];

            if self.buffer.len() == self.part_size.get() {
                log::debug!("Enough data to write a part to S3.");
                // Buffer is full, send it to the channel
                let old_buffer =
                    std::mem::replace(&mut self.buffer, Vec::with_capacity(self.part_size.get()));
                let new_part = bytes::Bytes::from(old_buffer);

                if let Some(tx) = &self.tx {
                    tx.blocking_send(new_part)
                        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to send part for multi-part upload. Has the receiver been dropped?"))?;
                } else {
                    panic!("It seems that the S3PartBuffer has been shutdown already, but we still have data to send. This is a bug in the code.");
                }
            }
        }

        Ok(total_written_bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // No-Op: Flushing a partial part will make the parts of unequal size. R2, for example,
        // requires all parts (except the last) to be the same size. The last part is always flushed
        // at shutdown().
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use common_io_config::S3Config;

    use crate::{object_io::ObjectSource, Result, S3LikeSource};

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
            .get(parquet_file_path, Some(0..10), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

        let first_bytes = client
            .get(parquet_file_path, Some(10..100), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

        let last_bytes = client
            .get(
                parquet_file_path,
                Some((all_bytes.len() - 10)..(all_bytes.len() + 10)),
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
