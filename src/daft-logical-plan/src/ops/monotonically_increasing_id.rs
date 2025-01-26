use std::sync::Arc;

use daft_core::prelude::*;

use crate::{
    logical_plan::{self},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct MonotonicallyIncreasingId {
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<Schema>,
    pub column_name: String,
    pub stats_state: StatsState,
}

impl MonotonicallyIncreasingId {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        column_name: Option<&str>,
    ) -> logical_plan::Result<Self> {
        let column_name = column_name.unwrap_or("id");

        let fields_with_id = std::iter::once(Field::new(column_name, DataType::UInt64))
            .chain(input.schema().fields.values().cloned())
            .collect();
        let schema_with_id = Schema::new(fields_with_id)?;

        Ok(Self {
            input,
            schema: Arc::new(schema_with_id),
            column_name: column_name.to_string(),
            stats_state: StatsState::NotMaterialized,
        })
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
