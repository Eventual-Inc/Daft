use std::sync::Arc;

use daft_core::prelude::*;
use daft_dsl::{expr::window::WindowFrame, ExprRef};

use crate::{
    logical_plan::{self},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Window {
    pub plan_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<Schema>,
    pub partition_by: Vec<ExprRef>,
    pub order_by: Vec<ExprRef>,
    pub frame: Option<WindowFrame>,
    pub stats_state: StatsState,
}

impl Window {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        partition_by: Vec<ExprRef>,
        order_by: Vec<ExprRef>,
        frame: Option<WindowFrame>,
    ) -> logical_plan::Result<Self> {
        let schema = input.schema();
        let stats_state = StatsState::NotMaterialized;
        Ok(Self {
            plan_id: None,
            input,
            schema,
            partition_by,
            order_by,
            frame,
            stats_state,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // Reuse input stats since window operations don't change cardinality
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("Window")];
        if !self.partition_by.is_empty() {
            res.push(format!("  partition_by={:?}", self.partition_by));
        }
        if !self.order_by.is_empty() {
            res.push(format!("  order_by={:?}", self.order_by));
        }
        if let Some(frame) = &self.frame {
            res.push(format!("  frame={:?}", frame));
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("  Stats = {}", stats));
        }
        res
    }
}
