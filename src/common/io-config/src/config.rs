use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    AzureConfig, GCSConfig, HTTPConfig, S3Config, huggingface::HuggingFaceConfig, tos::TosConfig,
    unity::UnityConfig,
};
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: S3Config,
    pub azure: AzureConfig,
    pub gcs: GCSConfig,
    pub http: HTTPConfig,
    pub unity: UnityConfig,
    pub hf: HuggingFaceConfig,
    /// disable suffix range requests, please use range with offset
    pub disable_suffix_range: bool,
    pub tos: TosConfig,
}

impl IOConfig {
    #[must_use]
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
        res.push(format!(
            "HTTP config = {{ {} }}",
            self.http.multiline_display().join(", ")
        ));
        res.push(format!(
            "Unity config = {{ {} }}",
            self.unity.multiline_display().join(", ")
        ));
        res.push(format!(
            "Hugging Face config = {{ {} }}",
            self.hf.multiline_display().join(", ")
        ));
        res.push(format!(
            "Disable suffix range = {}",
            self.disable_suffix_range
        ));
        res.push(format!(
            "TOS config = {{ {} }}",
            self.tos.multiline_display().join(", ")
        ));
        res
    }
}

impl Display for IOConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "IOConfig:
{}
{}
{}
{}
{}
{}
",
            self.s3, self.azure, self.gcs, self.tos, self.http, self.unity
        )
    }
}
