use std::sync::Arc;

use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    logical_plan::{self},
    stats::StatsState,
};

#[derive(Hash, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct MonotonicallyIncreasingId {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<Schema>,
    pub column_name: String,
    pub starting_offset: Option<u64>,
    pub stats_state: StatsState,
}

impl MonotonicallyIncreasingId {
    pub(crate) const DEFAULT_COLUMN_NAME: &str = "id";

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        column_name: Option<&str>,
        starting_offset: Option<u64>,
    ) -> logical_plan::Result<Self> {
        let column_name = column_name.unwrap_or(Self::DEFAULT_COLUMN_NAME);

        let input_schema = input.schema();
        let fields_with_id = std::iter::once(Field::new(column_name, DataType::UInt64))
            .chain(input_schema.fields().iter().cloned());
        let schema_with_id = Schema::new(fields_with_id);

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            schema: Arc::new(schema_with_id),
            column_name: column_name.to_string(),
            starting_offset,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("MonotonicallyIncreasingId")];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
