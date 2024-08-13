#[cfg(feature = "python")]
pub mod python;

mod azure;
mod config;
mod gcs;
mod http;
mod s3;

use std::fmt::{Debug, Display};

pub use crate::{
    azure::AzureConfig, config::IOConfig, gcs::GCSConfig, http::HTTPConfig, s3::S3Config,
    s3::S3Credentials,
};

#[derive(Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq, Hash)]
pub struct ObfuscatedString(String);

impl ObfuscatedString {
    pub fn as_string(&self) -> &String {
        &self.0
    }
}

impl Display for ObfuscatedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "** redacted **")
    }
}

impl Debug for ObfuscatedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "** redacted **")
    }
}

impl From<String> for ObfuscatedString {
    fn from(value: String) -> Self {
        ObfuscatedString(value)
    }
}
