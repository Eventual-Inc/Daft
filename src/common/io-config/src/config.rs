use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    AzureConfig, GCSConfig, HTTPConfig, S3Config, huggingface::HuggingFaceConfig, tos::TosConfig,
    unity::UnityConfig,
};
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: Option<S3Config>,
    pub azure: Option<AzureConfig>,
    pub gcs: Option<GCSConfig>,
    pub tos: Option<TosConfig>,
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

        if let Some(tos) = &self.tos {
            res.push(format!(
                "TOS config = {{ {} }}",
                tos.multiline_display().join(", ")
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
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "IOConfig:")?;

        if let Some(s3) = &self.s3 {
            writeln!(f)?;
            write!(f, "{}", s3)?;
        }

        if let Some(azure) = &self.azure {
            writeln!(f)?;
            write!(f, "{}", azure)?;
        }

        if let Some(gcs) = &self.gcs {
            writeln!(f)?;
            write!(f, "{}", gcs)?;
        }

        if let Some(tos) = &self.tos {
            writeln!(f)?;
            write!(f, "{}", tos)?;
        }

        if let Some(http) = &self.http {
            writeln!(f)?;
            write!(f, "{}", http)?;
        }

        Ok(())
    }
}
