#![feature(let_chains)]

#[cfg(feature = "python")]
pub mod python;

mod azure;
mod config;
mod gcs;
mod http;
mod s3;
mod unity;

use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Deserializer, Serialize};

pub use crate::{
    azure::AzureConfig,
    config::IOConfig,
    gcs::GCSConfig,
    http::HTTPConfig,
    s3::{S3Config, S3Credentials},
    unity::UnityConfig,
};

#[derive(Clone)]
pub struct ObfuscatedString(Secret<String>);

impl ObfuscatedString {
    #[must_use]
    pub fn as_string(&self) -> &String {
        self.0.expose_secret()
    }
}

impl PartialEq for ObfuscatedString {
    fn eq(&self, other: &Self) -> bool {
        self.0.expose_secret() == other.0.expose_secret()
    }
}

impl Eq for ObfuscatedString {}

impl Hash for ObfuscatedString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.expose_secret().hash(state);
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

impl Serialize for ObfuscatedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.expose_secret().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ObfuscatedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self(s.into()))
    }
}

impl From<String> for ObfuscatedString {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl std::str::FromStr for ObfuscatedString {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string().into()))
    }
}
