use std::{collections::HashMap, fs, time::Duration};

use async_trait::async_trait;
use aws_config::{
    endpoint::Endpoint, imds::Client, profile::Profile, provider_config::ProviderConfig,
    timeout::TimeoutConfig,
};
use futures::{StreamExt, TryStreamExt};
use s3::{config::Region, error::SdkError};

use crate::error::{DaftError, DaftResult};

use super::object_io::{GetResult, ObjectSource};

use aws_sdk_s3 as s3;

#[derive(Clone)]
pub struct S3LikeSource {
    client: s3::Client,
}

use crate::io::s3_like::s3::primitives::ByteStreamError;

impl From<ByteStreamError> for DaftError {
    fn from(error: ByteStreamError) -> Self {
        DaftError::IoError(Box::new(error))
    }
}

impl<E: Sync + Send + std::error::Error + 'static, R: std::fmt::Debug> From<SdkError<E, R>>
    for DaftError
{
    fn from(error: SdkError<E, R>) -> Self {
        log::warn!("{error:?}");
        DaftError::IoError(error.into_source().unwrap())
    }
}

impl From<anyhow::Error> for DaftError {
    fn from(error: anyhow::Error) -> Self {
        DaftError::IoError(error.into())
    }
}

use aws_config::sso::SsoCredentialsProvider;

async fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let region = Region::new("us-west-2");

    let conf = aws_config::load_from_env().await;

    let s3_conf = match endpoint.is_empty() {
        true => aws_sdk_s3::config::Builder::from(&conf)
            .region(region)
            .build(),
        false => aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_url(endpoint)
            .region(region)
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
    async fn get(&self, uri: String) -> DaftResult<GetResult> {
        let parsed = url::Url::parse(uri.as_str())?;
        let bucket = parsed.host_str().unwrap();
        let key = parsed.path();

        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(&key[1..])
            .send()
            .await?;

        let body = object.body;
        let stream = body.map_err(|e| e.into());
        Ok(GetResult::Stream(stream.boxed()))
    }
}
