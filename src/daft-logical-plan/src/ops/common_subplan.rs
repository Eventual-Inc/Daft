use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::LogicalPlanRef;

/// Explicit wrapper for shared subplans identified by Common Subplan Elimination.
///
/// All instances with the same `id` represent the same computation. The physical
/// executor should compute the subplan once (first encounter) and reuse the result
/// for all subsequent encounters with the same `id`.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CommonSubplan {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    /// Globally unique identifier for this shared subplan group.
    /// All CommonSubplan nodes with the same `id` refer to the same computation.
    pub id: usize,
    /// The shared subplan. Computed once and reused.
    pub subplan: LogicalPlanRef,
}

impl CommonSubplan {
    pub fn new(subplan: LogicalPlanRef, id: usize) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            id,
            subplan,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.subplan.schema()
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_materialized_stats(self, cfg: &DaftExecutionConfig) -> Self {
        Self {
            subplan: Arc::new(self.subplan.as_ref().clone().with_materialized_stats(cfg)),
            ..self
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("CommonSubplan id={}", self.id)];
        for line in self.subplan.multiline_display() {
            res.push(format!("  {}", line));
        }
        res
    }
}
