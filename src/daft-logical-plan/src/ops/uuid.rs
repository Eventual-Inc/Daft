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
pub struct Uuid {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<Schema>,
    pub column_name: String,
    pub stats_state: StatsState,
}

impl Uuid {
    pub(crate) const DEFAULT_COLUMN_NAME: &str = "uuid";

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        column_name: Option<&str>,
    ) -> logical_plan::Result<Self> {
        let column_name = column_name.unwrap_or(Self::DEFAULT_COLUMN_NAME);

        let input_schema = input.schema();
        let fields_with_uuid = std::iter::once(Field::new(column_name, DataType::Utf8))
            .chain(input_schema.fields().iter().cloned());
        let schema_with_uuid = Schema::new(fields_with_uuid);

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            schema: Arc::new(schema_with_uuid),
            column_name: column_name.to_string(),
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
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Uuid")];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
