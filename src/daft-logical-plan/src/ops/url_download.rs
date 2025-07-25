use std::sync::Arc;

use daft_core::prelude::*;
use daft_dsl::ExprRef;
use daft_functions_uri::UrlDownloadArgs;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

/// UrlDownload operator for downloading data from a URL.
///
/// It is currently unavailable in the Python API and only constructed by the
/// Daft logical optimizer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UrlDownload {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    /// An id for the node.
    pub node_id: Option<usize>,
    /// Upstream node.
    pub input: Arc<LogicalPlan>,
    /// Output schema.
    pub output_column: String,
    pub output_schema: Arc<Schema>,
    /// URL download arguments.
    pub args: UrlDownloadArgs<ExprRef>,
    /// The plan statistics.
    pub stats_state: StatsState,
}

impl UrlDownload {
    pub fn new(
        input: Arc<LogicalPlan>,
        args: UrlDownloadArgs<ExprRef>,
        output_column: String,
    ) -> Self {
        let mut output_schema = input.schema().as_ref().clone();
        output_schema.append(Field::new(output_column.clone(), DataType::Binary));

        Self {
            plan_id: None,
            node_id: None,
            input,
            output_column,
            output_schema: Arc::new(output_schema),
            args,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn input_column(&self) -> &ExprRef {
        &self.args.input
    }

    pub fn with_plan_id(mut self, id: usize) -> Self {
        self.plan_id = Some(id);
        self
    }

    pub fn with_node_id(mut self, id: usize) -> Self {
        self.node_id = Some(id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let num_rows = input_stats.approx_stats.num_rows;

        let approx_stats = ApproxStats {
            num_rows,
            size_bytes: input_stats.approx_stats.size_bytes + (8 * 1024 * num_rows),
            acc_selectivity: input_stats.approx_stats.acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("UrlDownload: args = {:?}", self.args));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
