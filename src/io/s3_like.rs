use async_trait::async_trait;

use futures::{StreamExt, TryStreamExt};
use s3::client::customize::Response;
use s3::error::SdkError;
use s3::operation::get_object::GetObjectError;
use snafu::{IntoError, ResultExt, Snafu};

use crate::error::{DaftError, DaftResult};

use super::object_io::{GetResult, ObjectSource};

use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::ByteStreamError;
#[derive(Clone)]
pub struct S3LikeSource {
    client: s3::Client,
}

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
enum Error {
    // #[snafu(display("Generic {} error: {:?}", store, source))]
    // Generic {
    //     store: &'static str,
    //     source: DynError,
    // },

    // #[snafu(display("Object at location {} not found: {:?}", path, source))]
    // NotFound {
    //     path: String,
    //     source: DynError,
    // },

    // #[snafu(display("Invalid Argument: {:?}", msg))]
    // InvalidArgument{msg: String},
    #[snafu(display(
        "Unable to open file {}: {}",
        path,
        s3::error::DisplayErrorContext(source)
    ))]
    UnableToOpenFile {
        path: String,
        source: SdkError<GetObjectError, Response>,
    },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: ByteStreamError,
    },

    #[snafu(display("Unable to convert URL \"{}\" to path", url))]
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        super::Error::Generic {
            store: "s3",
            source: error.into(),
        }
    }
}

// impl From<ByteStreamError> for DaftError {
//     fn from(error: ByteStreamError) -> Self {
//         DaftError::External(error.into())
//     }
// }

// impl<E: std::error::Error + 'static + Send + Sync, R: std::fmt::Debug + Send + Sync + 'static>
//     From<SdkError<E, R>> for Error
// where
//     Self: Send + Sync,
// {
//     fn from(error: SdkError<E, R>) -> Self {
//         Error::Generic{store: "s3", source: error.into()}
//     }
// }

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
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { url: uri })?;
        let bucket = parsed.host_str().unwrap();
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
