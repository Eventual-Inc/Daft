use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Flatten {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Flatten {
    pub(crate) fn new(input: Arc<PhysicalPlan>) -> Self {
        Self { input }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["Flatten".to_string()]
    }
}
