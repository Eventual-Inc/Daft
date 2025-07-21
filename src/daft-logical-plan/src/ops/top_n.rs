use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{exprs_to_schema, ExprRef};
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
/// The TopN operator is essentially a Sort followed by a Limit or Offset. But it
/// can be computed more efficiently as a single operator via an O(n) algorithm instead
/// of a O(n log n) algorithm for sorting.
///
/// It is currently unavailable in the Python API and only constructed by the
/// Daft logical optimizer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Offset on number of rows.
    pub offset: Option<u64>,
    /// Limit on number of rows.
    pub limit: Option<u64>,
    /// The plan statistics.
    pub stats_state: StatsState,
}

impl TopN {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<Self> {
        if sort_by.is_empty() {
            return Err(DaftError::InternalError(
                "TopN must be given at least one column/expression to sort by".to_string(),
            ))
            .context(CreationSnafu);
        }

        if offset.is_none() && limit.is_none() {
            return Err(DaftError::InternalError(
                "TopN node must have offset or limit".to_string(),
            ))
            .context(CreationSnafu);
        }

        if let (Some(o), Some(l)) = (offset, limit) {
            if o >= l {
                return Err(DaftError::InternalError(format!(
                    "TopN offset {} must be less than limit {}",
                    o, l
                )))
                .context(CreationSnafu);
            }
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
            offset,
            limit,
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
        let approx_num_rows = input_stats.approx_stats.num_rows;

        let limit_num_rows = match (self.offset, self.limit) {
            (Some(o), Some(l)) => (l - o) as usize,
            (Some(o), None) => approx_num_rows.saturating_sub(o as usize),
            (None, Some(l)) => l as usize,
            (None, None) => unreachable!(),
        };

        let limit_selectivity = if approx_num_rows == 0 {
            0.0
        } else {
            (limit_num_rows as f64 / approx_num_rows as f64).clamp(0.0, 1.0)
        };

        let approx_stats = ApproxStats {
            num_rows: limit_num_rows.min(approx_num_rows),
            size_bytes: if approx_num_rows > limit_num_rows {
                let est_bytes_per_row =
                    input_stats.approx_stats.size_bytes / approx_num_rows.max(1);
                limit_num_rows * est_bytes_per_row
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

        res.push(match (self.offset, self.limit) {
            (Some(o), Some(l)) => {
                format!(
                    "TopN: Sort by = {}, Num Rows = {}, Offset = {}",
                    pairs,
                    l.saturating_sub(o),
                    o
                )
            }
            (Some(o), None) => format!("TopN: Sort by = {}, Num Rows = N/A, Offset = {}", pairs, o),
            (None, Some(l)) => format!("TopN: Sort by = {}, Num Rows = {}", pairs, l),
            (None, None) => unreachable!(),
        });

        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
