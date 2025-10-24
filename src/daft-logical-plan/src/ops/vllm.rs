use std::sync::Arc;

use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::expr::VLLMExpr;
use daft_schema::{dtype::DataType, field::Field};
use serde::{Deserialize, Serialize};

use crate::{LogicalPlanRef, stats::StatsState};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VLLMProject {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub expr: VLLMExpr,
    pub input: LogicalPlanRef,

    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl VLLMProject {
    pub fn new(input: LogicalPlanRef, expr: VLLMExpr) -> Self {
        let output_fields = [
            input.schema().fields().to_vec(),
            vec![Field::new("daft_vllm_output", DataType::Utf8)],
        ]
        .concat();
        let output_schema = Arc::new(Schema::new(output_fields));

        Self {
            plan_id: None,
            node_id: None,
            input,
            expr,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        }
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
        let mut res = vec![format!("VLLM: {}", self.expr.to_string())];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
