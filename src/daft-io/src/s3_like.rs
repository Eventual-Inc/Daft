use async_trait::async_trait;

use aws_config::SdkConfig;
use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::config::{Credentials, Region};
use s3::error::{ProvideErrorMetadata, SdkError};
use s3::operation::get_object::GetObjectError;
use snafu::{IntoError, ResultExt, Snafu};
use url::ParseError;

use crate::config::S3Config;

use super::object_io::{GetResult, ObjectSource};

use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;

#[derive(Clone)]
pub(crate) struct S3LikeSource {
    client: s3::Client,
    s3_config: S3Config,
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
        }
    }
}

async fn build_client(config: &S3Config) -> S3LikeSource {
    let conf = aws_config::load_from_env().await;
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

    S3LikeSource {
        client: s3::Client::from_conf(s3_conf),
        s3_config: config.clone(),
    }
}

impl S3LikeSource {
    pub async fn new(config: &S3Config) -> Self {
        build_client(config).await
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
        let object = if let Some(key) = key.strip_prefix('/') {
            let request = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            match request {
                Ok(v) => Ok(v),
                Err(SdkError::ServiceError(err)) => match err.err() {
                    GetObjectError::Unhandled(unhandled) => match unhandled.meta().code() {
                        Some("PermanentRedirect") => {
                            let head = reqwest::Client::builder()
                                .build()
                                .unwrap()
                                .head(format! {"https://{bucket}.s3.amazonaws.com"})
                                .send()
                                .await
                                .unwrap();

                            // let head = reqwest::get().await.unwrap();
                            let head = head.headers();
                            let new_region = head.get("x-amz-bucket-region").unwrap();

                            // let buck_response = self.client.head_bucket().bucket(bucket).send().await.unwrap();
                            // buck_response.
                            let mut new_config = self.s3_config.clone();
                            new_config.region_name =
                                Some(String::from_utf8(new_region.as_bytes().to_vec()).unwrap());
                            let new_client = build_client(&new_config).await;
                            return new_client.get(uri).await;
                            // let builder = aws_sdk_s3::config::Builder:;
                            // let builder = aws_sdk_s3::config::Builder::from(conf);
                            // todo!("response {head:?} Set up redirect to new location!")
                        }
                        _ => Err(SdkError::ServiceError(err)),
                    },
                    &_ => Err(SdkError::ServiceError(err)),
                },
                Err(err) => Err(err),
            }
            .with_context(|_| UnableToOpenFileSnafu { path: uri })?
        } else {
            return Err(Error::NotAFile { path: uri.into() }.into());
        };
        let body = object.body;
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
            Some(object.content_length as usize),
        ))
    }
}
