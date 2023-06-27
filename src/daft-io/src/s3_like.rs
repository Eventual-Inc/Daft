use async_trait::async_trait;

use aws_config::SdkConfig;
use aws_credential_types::cache::ProvideCachedCredentials;
use aws_credential_types::provider::error::CredentialsError;
use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::config::{Credentials, Region};
use s3::error::{ProvideErrorMetadata, SdkError};
use s3::operation::get_object::GetObjectError;
use snafu::{IntoError, ResultExt, Snafu};
use url::ParseError;

use crate::config::S3Config;
use crate::SourceType;

use super::object_io::{GetResult, ObjectSource};
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub(crate) struct S3LikeSource {
    s3_client: s3::Client,
    s3_config: S3Config,
    http_client: reqwest::Client,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, s3::error::DisplayErrorContext(source)))]
    UnableToOpenFile {
        path: String,
        source: SdkError<GetObjectError, Response>,
    },

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

    #[snafu(display("Failed to load Credentials: {source}"))]
    FailedToLoadCredentials { source: CredentialsError },
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
            FailedToLoadCredentials { source } => super::Error::FailedToLoadCredentials {
                store: SourceType::S3,
                source: source.into(),
            },
        }
    }
}

async fn build_s3_client(config: &S3Config) -> super::Result<s3::Client> {
    let conf: SdkConfig = aws_config::load_from_env().await;

    let builder = aws_sdk_s3::config::Builder::from(&conf);
    let builder = match &config.endpoint_url {
        None => builder,
        Some(endpoint) => builder.endpoint_url(endpoint),
    };
    let builder = if let Some(region) = &config.region_name {
        builder.region(Region::new(region.to_owned()))
    } else if config.endpoint_url.is_none() && conf.region().is_none() {
        builder.region(Region::from_static("us-east-1"))
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
        panic!("Must provide both access_key and key_id when building S3-Like Client");
    } else {
        builder
    };

    let s3_conf = builder.build();

    s3_conf
        .credentials_cache()
        .provide_cached_credentials()
        .await
        .context(FailedToLoadCredentialsSnafu {})?;

    Ok(s3::Client::from_conf(s3_conf))
}

async fn build_client(config: &S3Config) -> super::Result<S3LikeSource> {
    let s3_client = build_s3_client(config).await?;

    Ok(S3LikeSource {
        s3_client,
        s3_config: config.clone(),
        http_client: reqwest::Client::builder().build().unwrap(),
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
    async fn get(&self, uri: &str) -> super::Result<GetResult> {
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
            let request = self
                .s3_client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            match request {
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
                            log::warn!("S3 Region of {uri} doesn't match that of the client: {:?}, Attempting to Resolve.", self.s3_client.conf().region().unwrap());
                            let head = self
                                .http_client
                                .head(format! {"https://{bucket}.s3.amazonaws.com"})
                                .send()
                                .await
                                .unwrap();

                            let head = head.headers();
                            let new_region = head.get("x-amz-bucket-region").unwrap();

                            let mut new_config = self.s3_config.clone();
                            new_config.region_name =
                                Some(String::from_utf8(new_region.as_bytes().to_vec()).unwrap());

                            let new_client = S3LikeSource::get_client(&new_config).await?;
                            log::warn!("Correct S3 Region of {uri} found: {:?}. Attempting GET in that region with new client", new_client.s3_client.conf().region().unwrap());
                            return new_client.get(uri).await;
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
