use std::sync::Arc;

use common_error::DaftError;
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use daft_dsl::{ExprRef, ExprResolver};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    stats::{ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unpivot {
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
            input,
            ids,
            values,
            variable_name,
            value_name,
            output_schema,
            stats_state: StatsState::NotMaterialized,
        }
    }

    // Similar to new, except that `try_new` is not given the output schema and instead extracts it.
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: &str,
        value_name: &str,
    ) -> logical_plan::Result<Self> {
        if values.is_empty() {
            return Err(DaftError::ValueError(
                "Unpivot requires at least one value column".to_string(),
            ))
            .context(CreationSnafu);
        }

        let expr_resolver = ExprResolver::default();

        let input_schema = input.schema();
        let (values, values_fields) = expr_resolver
            .resolve(values, &input_schema)
            .context(CreationSnafu)?;

        let value_dtype = values_fields
            .iter()
            .map(|f| f.dtype.clone())
            .try_reduce(|a, b| try_get_supertype(&a, &b))
            .context(CreationSnafu)?
            .unwrap();

        let variable_field = Field::new(variable_name, DataType::Utf8);
        let value_field = Field::new(value_name, value_dtype);

        let (ids, ids_fields) = expr_resolver
            .resolve(ids, &input_schema)
            .context(CreationSnafu)?;

        let output_fields = ids_fields
            .into_iter()
            .chain([variable_field, value_field])
            .collect::<Vec<_>>();

        let output_schema = Schema::new(output_fields).context(CreationSnafu)?.into();

        Ok(Self {
            input,
            ids,
            values,
            variable_name: variable_name.to_string(),
            value_name: value_name.to_string(),
            output_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let num_values = self.values.len();
        let approx_stats = ApproxStats {
            lower_bound_rows: input_stats.approx_stats.lower_bound_rows * num_values,
            upper_bound_rows: input_stats
                .approx_stats
                .upper_bound_rows
                .map(|v| v * num_values),
            lower_bound_bytes: input_stats.approx_stats.lower_bound_bytes,
            upper_bound_bytes: input_stats.approx_stats.upper_bound_bytes,
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
