use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    AzureConfig, GCSConfig, HTTPConfig, S3Config, huggingface::HuggingFaceConfig,
    unity::UnityConfig,
};
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: Option<S3Config>,
    pub azure: Option<AzureConfig>,
    pub gcs: Option<GCSConfig>,
    pub http: Option<HTTPConfig>,
    pub unity: Option<UnityConfig>,
    pub hf: Option<HuggingFaceConfig>,
}

impl IOConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(s3) = &self.s3 {
            res.push(format!(
                "S3 config = {{ {} }}",
                s3.multiline_display().join(", ")
            ));
        }
        if let Some(azure) = &self.azure {
            res.push(format!(
                "Azure config = {{ {} }}",
                azure.multiline_display().join(", ")
            ));
        }
        if let Some(gcs) = &self.gcs {
            res.push(format!(
                "GCS config = {{ {} }}",
                gcs.multiline_display().join(", ")
            ));
        }
        if let Some(http) = &self.http {
            res.push(format!(
                "HTTP config = {{ {} }}",
                http.multiline_display().join(", ")
            ));
        }
        if let Some(unity) = &self.unity {
            res.push(format!(
                "Unity config = {{ {} }}",
                unity.multiline_display().join(", ")
            ));
        }
        if let Some(hf) = &self.hf {
            res.push(format!(
                "Hugging Face config = {{ {} }}",
                hf.multiline_display().join(", ")
            ));
        }
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
{}",
            self.s3.clone().unwrap_or_default(),
            self.azure.clone().unwrap_or_default(),
            self.gcs.clone().unwrap_or_default(),
            self.http.clone().unwrap_or_default(),
            self.unity.clone().unwrap_or_default(),
            self.hf.clone().unwrap_or_default(),
        )
    }
}
