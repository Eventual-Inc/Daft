use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Concat {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub other: PhysicalPlanRef,
}

impl Concat {
    pub(crate) fn new(input: PhysicalPlanRef, other: PhysicalPlanRef) -> Self {
        Self { input, other }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["Concat".to_string()]
    }
}

impl_default_tree_display!(Concat);
