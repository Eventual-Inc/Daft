use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_schema::schema::{Schema, SchemaRef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    logical_plan::{self},
    stats::StatsState,
};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Pivot {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub group_by: Vec<ExprRef>,
    pub pivot_column: ExprRef,
    pub value_column: ExprRef,
    pub aggregation: AggExpr,
    pub names: Vec<String>,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Pivot {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        aggregation: ExprRef,
        names: Vec<String>,
    ) -> logical_plan::Result<Self> {
        let Expr::Agg(agg_expr) = aggregation.as_ref() else {
            return Err(DaftError::ValueError(format!(
                "Pivot only supports using top level aggregation expressions, received {aggregation}",
            ))
            .into());
        };

        let output_schema = {
            let agg_dtype = agg_expr.to_field(&input.schema())?.dtype;
            let pivot_value_fields = names.iter().map(|f| Field::new(f, agg_dtype.clone()));

            let group_by_fields = group_by
                .iter()
                .map(|expr| expr.to_field(&input.schema()))
                .collect::<DaftResult<Vec<_>>>()?;

            let fields = group_by_fields.into_iter().chain(pivot_value_fields);
            Schema::new(fields).into()
        };

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation: agg_expr.clone(),
            names,
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
        // TODO(desmond): Pivoting does affect cardinality, but for now we keep the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Pivot:".to_string());
        res.push(format!(
            "Group by = {}",
            self.group_by.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Pivot column: {}", self.pivot_column));
        res.push(format!("Value column: {}", self.value_column));
        res.push(format!("Aggregation: {}", self.aggregation));
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
