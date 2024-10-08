use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GCSConfig {
    pub project_id: Option<String>,
    pub credentials: Option<ObfuscatedString>,
    pub token: Option<String>,
    pub anonymous: bool,
}

impl GCSConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(project_id) = &self.project_id {
            res.push(format!("Project ID = {project_id}"));
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res
    }
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
