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

    pub(crate) fn materialize_stats(&self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let new_input = self.input.materialize_stats();
        let new_other = self.other.materialize_stats();
        let approx_stats =
            &new_input.get_stats().approx_stats + &new_other.get_stats().approx_stats;
        let stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        Self {
            input: Arc::new(new_input),
            other: Arc::new(new_other),
            stats_state,
        }
    }
}
