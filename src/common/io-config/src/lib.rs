#[cfg(feature = "python")]
pub mod python;

mod azure;
mod config;
mod gcs;
mod s3;

pub use crate::{azure::AzureConfig, config::IOConfig, gcs::GCSConfig, s3::S3Config};
