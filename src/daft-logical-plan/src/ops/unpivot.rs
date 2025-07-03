use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Unpivot {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub ids: Vec<ExprRef>,
    pub values: Vec<ExprRef>,
    pub variable_name: String,
    pub value_name: String,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Unpivot {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: String,
        value_name: String,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            ids,
            values,
            variable_name,
            value_name,
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

    // Similar to new, except that `try_new` is not given the output schema and instead extracts it.
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: String,
        value_name: String,
    ) -> logical_plan::Result<Self> {
        if values.is_empty() {
            return Err(DaftError::ValueError(
                "Unpivot requires at least one value column".to_string(),
            ))
            .context(CreationSnafu);
        }

        let value_dtype = values
            .iter()
            .map(|expr| Ok(expr.to_field(&input.schema())?.dtype))
            .reduce(|a, b| try_get_supertype(&a?, &b?))
            .unwrap()?;

        let variable_field = Field::new(&variable_name, DataType::Utf8);
        let value_field = Field::new(&value_name, value_dtype);

        let output_fields = ids
            .iter()
            .map(|id| id.to_field(&input.schema()))
            .chain([Ok(variable_field), Ok(value_field)])
            .collect::<DaftResult<Vec<_>>>()?;

        let output_schema = Schema::new(output_fields).into();

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            ids,
            values,
            variable_name,
            value_name,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let num_values = self.values.len();
        let approx_stats = ApproxStats {
            num_rows: input_stats.approx_stats.num_rows * num_values,
            size_bytes: input_stats.approx_stats.size_bytes,
            acc_selectivity: input_stats.approx_stats.acc_selectivity * num_values as f64,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Unpivot: {}",
            self.values.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Ids = {}",
            self.ids.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Schema = {}", self.output_schema.short_string()));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
