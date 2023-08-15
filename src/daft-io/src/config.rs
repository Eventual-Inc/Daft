use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct S3Config {
    pub region_name: Option<String>,
    pub endpoint_url: Option<String>,
    pub key_id: Option<String>,
    pub session_token: Option<String>,
    pub access_key: Option<String>,
    pub retry_initial_backoff_ms: u32,
    pub num_tries: u32,
    pub anonymous: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            region_name: None,
            endpoint_url: None,
            key_id: None,
            session_token: None,
            access_key: None,
            retry_initial_backoff_ms: 1000,
            num_tries: 5,
            anonymous: false,
        }
    }
}

impl Display for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "S3Config
    region_name: {:?}
    endpoint_url: {:?}
    key_id: {:?}
    session_token: {:?},
    access_key: {:?}
    retry_initial_backoff_ms: {:?},
    num_tries: {:?},
    anonymous: {}",
            self.region_name,
            self.endpoint_url,
            self.key_id,
            self.session_token,
            self.access_key,
            self.retry_initial_backoff_ms,
            self.num_tries,
            self.anonymous
        )
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AzureConfig {
    pub storage_account: Option<String>,
    pub access_key: Option<String>,
    pub anonymous: bool,
}

impl Display for AzureConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "AzureConfig
    storage_account: {:?}
    access_key: {:?}
    anonymous: {:?}",
            self.storage_account, self.access_key, self.anonymous
        )
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GCSConfig {
    pub project_id: Option<String>,
    pub anonymous: bool,
}

impl Display for GCSConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "GCSConfig
    project_id: {:?}
    anonymous: {:?}",
            self.project_id, self.anonymous
        )
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: S3Config,
    pub azure: AzureConfig,
    pub gcs: GCSConfig,
}

impl Display for IOConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "IOConfig:
{}
{}
{}",
            self.s3, self.azure, self.gcs
        )
    }
}
