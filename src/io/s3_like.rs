use async_trait::async_trait;

use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::error::SdkError;
use s3::operation::get_object::GetObjectError;
use snafu::{IntoError, OptionExt, ResultExt, Snafu};
use url::ParseError;

use super::object_io::{GetResult, ObjectSource};

use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;
#[derive(Clone)]
pub struct S3LikeSource {
    client: s3::Client,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
        "Unable to open {}: {}",
        path,
        s3::error::DisplayErrorContext(source)
    ))]
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
                err => {
                    super::Error::UnableToOpenFile {
                        path,
                        source: err.into(),
                    }
                }
            },
            InvalidUrl { path, source } => super::Error::InvalidUrl {
                path,
                source,
            },
            UnableToReadBytes { path, source } => super::Error::UnableToReadBytes {
                path,
                source: source.into(),
            },
        }
    }
}

async fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let conf = aws_config::load_from_env().await;

    let s3_conf = match endpoint.is_empty() {
        true => aws_sdk_s3::config::Builder::from(&conf).build(),
        false => aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_url(endpoint)
            .build(),
    };

    s3::Client::from_conf(s3_conf)
}

impl S3LikeSource {
    pub async fn new() -> Self {
        S3LikeSource {
            client: build_client("").await,
        }
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

        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(&key[1..])
            .send()
            .await
            .with_context(|_| UnableToOpenFileSnafu { path: uri })?;
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
