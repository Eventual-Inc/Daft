use std::sync::Arc;

use common_error::DaftError;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    stats::{PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Concat {
    // Upstream nodes.
    pub input: Arc<LogicalPlan>,
    pub other: Arc<LogicalPlan>,
    pub stats_state: StatsState,
}

impl Concat {
    pub(crate) fn new(input: Arc<LogicalPlan>, other: Arc<LogicalPlan>) -> Self {
        Self {
            input,
            other,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        other: Arc<LogicalPlan>,
    ) -> logical_plan::Result<Self> {
        let self_schema = input.schema();
        let other_schema = other.schema();
        if self_schema != other_schema {
            return Err(DaftError::ValueError(format!(
                "Both DataFrames must have the same schema to concatenate them, but got: {}, {}",
                self_schema, other_schema
            )))
            .context(CreationSnafu);
        }
        Ok(Self {
            input,
            other,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        let other_stats = self.other.materialized_stats();
        let approx_stats = &input_stats.approx_stats + &other_stats.approx_stats;
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Concat")];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
