use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::ExprRef;
use daft_stats::plan_stats::{calculate::calculate_filter_stats, StatsState};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Filter {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // The Boolean expression to filter on.
    pub predicate: ExprRef,
    pub stats_state: StatsState,
}

impl Filter {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        predicate: ExprRef,
    ) -> logical_plan::Result<Self> {
        let dtype = predicate.to_field(&input.schema())?.dtype;

        if !matches!(dtype, DataType::Boolean) {
            return Err(DaftError::ValueError(format!(
                "Expected expression {predicate} to resolve to type Boolean, but received: {}",
                dtype
            )))
            .context(CreationSnafu);
        }
        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            predicate,
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
        let stats = calculate_filter_stats(input_stats, &self.predicate, &self.input.schema());
        self.stats_state = StatsState::Materialized(stats.into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Filter: {}", self.predicate)];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
