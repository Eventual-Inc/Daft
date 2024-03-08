use crate::{physical_plan::PhysicalPlanRef, sink_info::OutputFileInfo};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OverwriteFiles {
    pub input: PhysicalPlanRef,
    pub file_info: OutputFileInfo,
}

impl OverwriteFiles {
    pub(crate) fn new(input: PhysicalPlanRef, file_info: OutputFileInfo) -> Self {
        Self { input, file_info }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec!["OverwriteFiles".to_string()]
    }
}
