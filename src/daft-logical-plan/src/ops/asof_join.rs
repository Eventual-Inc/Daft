use std::sync::Arc;

use daft_core::join::JoinType;
use daft_dsl::{ExprRef, join::infer_join_schema};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan, LogicalPlanRef,
    logical_plan::{self},
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct AsofJoin {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub left_by: Vec<ExprRef>,
    pub right_by: Vec<ExprRef>,
    pub left_on: ExprRef,
    pub right_on: ExprRef,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl AsofJoin {
    pub(crate) fn try_new(
        left: LogicalPlanRef,
        right: LogicalPlanRef,
        left_by: Vec<ExprRef>,
        right_by: Vec<ExprRef>,
        left_on: ExprRef,
        right_on: ExprRef,
    ) -> logical_plan::Result<Self> {
        let output_schema = infer_join_schema(&left.schema(), &right.schema(), JoinType::Left)?;

        Ok(Self {
            plan_id: None,
            node_id: None,
            left,
            right,
            left_by,
            right_by,
            left_on,
            right_on,
            output_schema,
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
        let left_stats = self.left.materialized_stats();
        let right_stats = self.right.materialized_stats();
        let left_num_rows =
            left_stats.approx_stats.num_rows as f64 * right_stats.approx_stats.acc_selectivity;
        let right_num_rows =
            right_stats.approx_stats.num_rows as f64 * left_stats.approx_stats.acc_selectivity;
        let left_size =
            left_stats.approx_stats.size_bytes as f64 * right_stats.approx_stats.acc_selectivity;
        let right_size =
            right_stats.approx_stats.size_bytes as f64 * left_stats.approx_stats.acc_selectivity;
        let approx_stats = ApproxStats {
            num_rows: left_num_rows.max(right_num_rows).ceil() as usize,
            size_bytes: left_size.max(right_size).ceil() as usize,
            acc_selectivity: left_stats.approx_stats.acc_selectivity
                * right_stats.approx_stats.acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec!["AsofJoin: direction = backward".to_string()];
        res.push(format!(
            "left_by = [{}]",
            self.left_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res.push(format!(
            "right_by = [{}]",
            self.right_by
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        res.push(format!("left_on = {}", self.left_on));
        res.push(format!("right_on = {}", self.right_on));
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
