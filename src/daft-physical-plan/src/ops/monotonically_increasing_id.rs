use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::PhysicalPlan;

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

crate::impl_default_tree_display!(MonotonicallyIncreasingId);
