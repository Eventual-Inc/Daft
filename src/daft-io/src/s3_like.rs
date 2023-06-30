use async_trait::async_trait;

use crate::config::S3Config;
use crate::SourceType;
use aws_config::SdkConfig;
use aws_credential_types::cache::ProvideCachedCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_sig_auth::signer::SigningRequirements;
use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::config::{Credentials, Region};
use s3::error::{ProvideErrorMetadata, SdkError};
use s3::operation::get_object::GetObjectError;
use snafu::{IntoError, ResultExt, Snafu};
use url::ParseError;

use super::object_io::{GetResult, ObjectSource};
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::ops::Range;
use std::string::FromUtf8Error;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub(crate) struct S3LikeSource {
    s3_client: s3::Client,
    s3_config: S3Config,
    http_client: reqwest::Client,
    anonymous: bool,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, s3::error::DisplayErrorContext(source)))]
    UnableToOpenFile {
        path: String,
        source: SdkError<GetObjectError, Response>,
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

    #[snafu(display("Unable to load Credentials: {}", source))]
    UnableToLoadCredentials { source: CredentialsError },

    #[snafu(display("Unable to create http client. {}", source))]
    UnableToCreateClient { source: reqwest::Error },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8 { path: String, source: FromUtf8Error },
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
            err => super::Error::Generic {
                store: SourceType::S3,
                source: err.into(),
            },
        }
    }
}

async fn build_client(config: &S3Config) -> super::Result<S3LikeSource> {
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
        Some(endpoint) => builder.endpoint_url(endpoint),
    };
    let builder = if let Some(region) = &config.region_name {
        builder.region(Region::new(region.to_owned()))
    } else if conf.region().is_none() && config.region_name.is_none() {
        builder.region(DEFAULT_REGION)
    } else {
        builder
    };

    let builder = if config.access_key.is_some() && config.key_id.is_some() {
        let creds = Credentials::from_keys(
            config.access_key.clone().unwrap(),
            config.key_id.clone().unwrap(),
            None,
        );
        builder.credentials_provider(creds)
    } else if config.access_key.is_some() || config.key_id.is_some() {
        return Err(super::Error::InvalidArgument {
            msg: "Must provide both access_key and key_id when building S3-Like Client".to_string(),
        });
    } else {
        builder
    };

    let s3_conf = builder.build();
    if !config.anonymous {
        use CredentialsError::*;
        match s3_conf
            .credentials_cache()
            .provide_cached_credentials()
            .await {
            Ok(_) => Ok(()),
            Err(err @ CredentialsNotLoaded(..)) => {
                log::warn!("S3 Credentials not provided or found when making client for {}! Reverting to Anonymous mode. {err}", s3_conf.region().unwrap_or(&DEFAULT_REGION));
                anonymous = true;
                Ok(())
            },
            Err(err) => Err(err),
        }.with_context(|_| UnableToLoadCredentialsSnafu {})?;
    };

    let client = s3::Client::from_conf(s3_conf);

    Ok(S3LikeSource {
        s3_client: client,
        s3_config: config.clone(),
        http_client: reqwest::Client::builder()
            .build()
            .with_context(|_| UnableToCreateClientSnafu {})?,
        anonymous: anonymous,
    })
}

lazy_static! {
    static ref S3_CLIENT_MAP: RwLock<HashMap<S3Config, Arc<S3LikeSource>>> =
        RwLock::new(HashMap::new());
}

impl S3LikeSource {
    pub async fn get_client(config: &S3Config) -> super::Result<Arc<S3LikeSource>> {
        {
            if let Some(client) = S3_CLIENT_MAP.read().unwrap().get(config) {
                return Ok(client.clone());
            }
        }

        let new_client = Arc::new(build_client(config).await?);

        let mut w_handle = S3_CLIENT_MAP.write().unwrap();
        if w_handle.get(config).is_none() {
            w_handle.insert(config.clone(), new_client.clone());
        }
        Ok(w_handle.get(config).unwrap().clone())
    }
}

#[async_trait]
impl ObjectSource for S3LikeSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let bucket = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();
        if let Some(key) = key.strip_prefix('/') {
            let request = self.s3_client.get_object().bucket(bucket).key(key);
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
                    Ok(GetResult::Stream(stream, Some(v.content_length as usize)))
                }
                Err(SdkError::ServiceError(err)) => match err.err() {
                    GetObjectError::Unhandled(unhandled) => match unhandled.meta().code() {
                        Some("PermanentRedirect") if self.s3_config.endpoint_url.is_none() => {
                            log::warn!("S3 Region of {uri} doesn't match that of the client: {:?}, Attempting to Resolve.", self.s3_client.conf().region().map_or("", |v| v.as_ref()));
                            let head = self
                                .http_client
                                .head(format! {"https://{bucket}.s3.amazonaws.com"})
                                .send()
                                .await
                                .with_context(|_| UnableToQueryRegionSnafu::<String> {
                                    path: uri.into(),
                                })?;
                            const REGION_HEADER: &str = "x-amz-bucket-region";
                            let headers = head.headers();
                            let new_region =
                                headers.get(REGION_HEADER).ok_or(Error::MissingHeader {
                                    path: uri.into(),
                                    header: REGION_HEADER.into(),
                                })?;

                            let mut new_config = self.s3_config.clone();
                            new_config.region_name = Some(
                                String::from_utf8(new_region.as_bytes().to_vec()).with_context(
                                    |_| UnableToParseUtf8Snafu::<String> { path: uri.into() },
                                )?,
                            );

                            let new_client = S3LikeSource::get_client(&new_config).await?;
                            log::warn!("Correct S3 Region of {uri} found: {:?}. Attempting GET in that region with new client", new_client.s3_client.conf().region().map_or("", |v| v.as_ref()));
                            return new_client.get(uri, range).await;
                        }
                        _ => Err(UnableToOpenFileSnafu { path: uri }
                            .into_error(SdkError::ServiceError(err))
                            .into()),
                    },
                    &_ => Err(UnableToOpenFileSnafu { path: uri }
                        .into_error(SdkError::ServiceError(err))
                        .into()),
                },
                Err(err) => Err(UnableToOpenFileSnafu { path: uri }.into_error(err).into()),
            }
        } else {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::object_io::ObjectSource;
    use crate::S3LikeSource;
    use crate::{config::S3Config, Result};
    use tokio;

    #[tokio::test]
    async fn test_full_get_from_s3() -> Result<()> {
        let parquet_file_path = "s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let mut config = S3Config::default();
        config.anonymous = true;
        let client = S3LikeSource::get_client(&config).await?;
        let parquet_file = client.get(parquet_file_path, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);
        Ok(())
    }
}
