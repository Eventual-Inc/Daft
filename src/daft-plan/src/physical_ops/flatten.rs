use serde::{Deserialize, Serialize};

use crate::physical_plan::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Flatten {
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl Flatten {
    pub(crate) fn new(input: PhysicalPlanRef) -> Self {
        Self { input }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["Flatten".to_string()]
    }
}
crate::impl_default_tree_display!(Flatten);
