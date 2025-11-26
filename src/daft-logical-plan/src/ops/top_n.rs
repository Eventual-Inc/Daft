use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, exprs_to_schema};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, LogicalPlan, Result},
    stats::{ApproxStats, PlanStats, StatsState},
};

/// TopN operator for computing the largest / smallest N rows based on a set of
/// sort keys and orderings.
///
/// The TopN operator is essentially a Sort followed by a Limit with optional Offset. But it
/// can be computed more efficiently as a single operator via an O(n) algorithm instead
/// of a O(n log n) algorithm for sorting.
///
/// It is currently unavailable in the Python API and only constructed by the
/// Daft logical optimizer.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct TopN {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    /// Upstream node.
    pub input: Arc<LogicalPlan>,
    /// Sort properties.
    pub sort_by: Vec<ExprRef>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
    /// Limit on number of rows.
    pub limit: u64,
    /// Offset on number of rows. This is optional, it's equivalent to offset = 0 if not passed.
    pub offset: Option<u64>,
    /// The plan statistics.
    pub stats_state: StatsState,
}

impl TopN {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: u64,
        offset: Option<u64>,
    ) -> Result<Self> {
        if sort_by.is_empty() {
            return Err(DaftError::InternalError(
                "TopN must be given at least one column/expression to sort by".to_string(),
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
            limit,
            offset,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, id: usize) -> Self {
        self.plan_id = Some(id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let limit = (self.limit + self.offset.unwrap_or(0)) as usize;
        let limit_selectivity = if input_stats.approx_stats.num_rows > limit {
            if input_stats.approx_stats.num_rows == 0 {
                0.0
            } else {
                limit as f64 / input_stats.approx_stats.num_rows as f64
            }
        } else {
            1.0
        };

        let approx_stats = ApproxStats {
            num_rows: limit.min(input_stats.approx_stats.num_rows),
            size_bytes: if input_stats.approx_stats.num_rows > limit {
                let est_bytes_per_row =
                    input_stats.approx_stats.size_bytes / input_stats.approx_stats.num_rows.max(1);
                limit * est_bytes_per_row
            } else {
                input_stats.approx_stats.size_bytes
            },
            acc_selectivity: input_stats.approx_stats.acc_selectivity * limit_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
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

        res.push(match &self.offset {
            Some(offset) => {
                format!(
                    "TopN: Sort by = {}, Num Rows = {}, Offset = {}",
                    pairs, self.limit, offset
                )
            }
            None => format!("TopN: Sort by = {}, Num Rows = {}", pairs, self.limit),
        });

        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
