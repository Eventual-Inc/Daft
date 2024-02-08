use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MonotonicallyIncreasingId {
    pub input: Arc<PhysicalPlan>,
    pub column_name: String,
}

impl MonotonicallyIncreasingId {
    pub(crate) fn new(input: Arc<PhysicalPlan>, column_name: &str) -> Self {
        Self {
            input,
            column_name: column_name.to_owned(),
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["MonotonicallyIncreasingId".to_string()]
    }
}
