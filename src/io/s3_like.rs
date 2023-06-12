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

// impl<E: Sync + Send + std::error::Error + 'static, R : std::fmt::Debug > From<SdkError<E,R>> for DaftError {
//     fn from(error: SdkError<E,R>) -> Self {
//         log::warn!("{error:?}");
//         DaftError::IoError(error.into_source().unwrap())
//     }
// }

impl From<anyhow::Error> for DaftError {
    fn from(error: anyhow::Error) -> Self {
        DaftError::IoError(error.into())
    }
}

use aws_config::sso::SsoCredentialsProvider;

fn parse_profile() -> SsoCredentialsProvider {
    let map = ini::ini!("/Users/sammy/.aws/config");
    let default_profile = map.get("default").unwrap();
    let sso_role_name = default_profile
        .get("sso_role_name")
        .unwrap()
        .clone()
        .unwrap_or_default();
    let sso_region = default_profile
        .get("sso_region")
        .unwrap()
        .clone()
        .unwrap_or_default();
    let sso_start_url = default_profile
        .get("sso_start_url")
        .unwrap()
        .clone()
        .unwrap_or_default();
    let sso_account_id = default_profile
        .get("sso_account_id")
        .unwrap()
        .clone()
        .unwrap_or_default();
    SsoCredentialsProvider::builder()
        .role_name(sso_role_name)
        .region(Region::new(sso_region))
        .start_url(sso_start_url)
        .account_id(sso_account_id)
        .build()
}

fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let region = Region::new("us-west-2");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let conf = rt.block_on(async { aws_config::load_from_env().await });

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
    pub fn new() -> Self {
        S3LikeSource {
            client: build_client(""),
        }
    }
}

#[async_trait]
impl ObjectSource for S3LikeSource {
    async fn get(&self, uri: String) -> anyhow::Result<GetResult> {
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
