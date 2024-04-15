use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::{AzureConfig, GCSConfig, S3Config};
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: S3Config,
    pub azure: AzureConfig,
    pub gcs: GCSConfig,
}

impl IOConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "S3 config = {{ {} }}",
            self.s3.multiline_display().join(", ")
        ));
        res.push(format!(
            "Azure config = {{ {} }}",
            self.azure.multiline_display().join(", ")
        ));
        res.push(format!(
            "GCS config = {{ {} }}",
            self.gcs.multiline_display().join(", ")
        ));
        res
    }

    pub fn from_env() -> Self {
        Self {
            s3: S3Config::from_env(),
            azure: AzureConfig::from_env(),
            gcs: GCSConfig::from_env(),
        }
    }
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
