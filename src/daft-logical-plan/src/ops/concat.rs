use std::sync::Arc;

use common_error::DaftError;
use daft_stats::plan_stats::{calculate::calculate_concat_stats, StatsState};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Concat {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream nodes.
    pub input: Arc<LogicalPlan>,
    pub other: Arc<LogicalPlan>,
    pub stats_state: StatsState,
}

impl Concat {
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
            plan_id: None,
            node_id: None,
            input,
            other,
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
        let other_stats = self.other.materialized_stats();
        let stats = calculate_concat_stats(input_stats, other_stats);
        self.stats_state = StatsState::Materialized(stats.into());
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
