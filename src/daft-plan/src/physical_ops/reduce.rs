use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReduceMerge {
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl ReduceMerge {
    pub(crate) fn new(input: PhysicalPlanRef) -> Self {
        Self { input }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["ReduceMerge".to_string()]
    }
}
