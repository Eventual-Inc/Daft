use std::collections::HashMap;
use std::sync::Arc;

use common_io_config::IOConfig;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_core::prelude::SchemaRef;
use daft_logical_plan::InMemoryInfo;
use serde::{Deserialize, Serialize};

/// Type of input expected by a source, containing the actual input data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputType {
    ScanTask(Arc<Vec<ScanTaskLikeRef>>),
    InMemory(InMemoryInfo),
    GlobPaths(Arc<Vec<String>>),
}

/// Specification for an input source in the plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSpec {
    pub source_id: String,
    pub input_type: InputType,
    pub schema: SchemaRef,
    pub pushdowns: Option<Pushdowns>,
    pub io_config: Option<common_io_config::IOConfig>,
}

/// Result of translating a logical plan, containing both the physical plan and input specifications
#[derive(Debug, Clone)]
pub struct TranslationResult {
    pub plan: crate::LocalPhysicalPlanRef,
    pub input_specs: HashMap<String, InputSpec>,
}

