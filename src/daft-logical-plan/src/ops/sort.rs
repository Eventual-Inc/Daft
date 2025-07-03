use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{exprs_to_schema, ExprRef};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{logical_plan, logical_plan::CreationSnafu, stats::StatsState, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sort {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub stats_state: StatsState,
}

impl Sort {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> logical_plan::Result<Self> {
        if sort_by.is_empty() {
            return Err(DaftError::ValueError(
                "df.sort() must be given at least one column/expression to sort by".to_string(),
            ))
            .context(CreationSnafu);
        }

        // TODO(Kevin): make sort by expression names unique so that we can do things like sort(col("a"), col("a") + col("b"))
        let sort_by_schema = exprs_to_schema(&sort_by, input.schema())?;

        for (field, expr) in sort_by_schema.into_iter().zip(sort_by.iter()) {
            // Disallow sorting by null, binary, and boolean columns.
            // TODO(Clark): This is a port of an existing constraint, we should look at relaxing this.
            if let dt @ (DataType::Null | DataType::Binary) = &field.dtype {
                return Err(DaftError::ValueError(format!(
                    "Cannot sort on expression {expr} with type: {dt}",
                )))
                .context(CreationSnafu);
            }
        }
        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            sort_by,
            descending,
            nulls_first,
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
        // Sorting does not affect cardinality.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        // Must have at least one expression to sort by.
        assert!(!self.sort_by.is_empty());
        let pairs = self
            .sort_by
            .iter()
            .zip(self.descending.iter())
            .zip(self.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        res.push(format!("Sort: Sort by = {}", pairs));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
