use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{expr::window::WindowSpec, WindowExpr};
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{LogicalPlan, Result},
    stats::StatsState,
};

/// Window operator for computing window functions.
///
/// The Window operator represents window function operations in the logical plan.
/// When a user calls an expression like `df.select(col("a").sum().over(window))`,
/// it gets translated into this operator as follows:
///
/// 1. The aggregation function `col("a").sum()` is stored in the `window_functions` vector
/// 2. The window specification (partition by, order by, frame) is stored in the `window_spec` field
///
/// For example, `df.select(col("a").sum().over(window.partition_by("b")))` becomes:
/// ```
/// Window {
///   window_functions = [col("a").sum()],
///   window_spec = WindowSpec { partition_by: [col("b")], ... }
/// }
/// ```
///
/// Multiple window function expressions can be stored in a single Window operator
/// as long as they share the same window specification.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Window {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    /// The input plan.
    pub input: Arc<LogicalPlan>,
    /// The window functions to compute.
    pub window_functions: Vec<WindowExpr>,
    /// The window function names to map to the output schema.
    pub aliases: Vec<String>,
    /// The window specification (partition by, order by, frame, etc.)
    pub window_spec: WindowSpec,
    /// The output schema.
    pub schema: Arc<Schema>,
    /// The plan statistics.
    pub stats_state: StatsState,
}

impl Window {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        window_functions: Vec<WindowExpr>,
        aliases: Vec<String>,
        window_spec: WindowSpec,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields = input_schema
            .into_iter()
            .cloned()
            .map(Ok)
            .chain(
                aliases
                    .iter()
                    .zip(window_functions.iter())
                    .map(|(name, expr)| {
                        let dtype = expr.to_field(&input_schema)?.dtype;
                        Ok(Field::new(name, dtype))
                    }),
            )
            .collect::<DaftResult<Vec<_>>>()?;

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            window_functions,
            aliases,
            window_spec,
            schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_materialized_stats(mut self) -> Self {
        // For now, just use the input's stats as an approximation
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn with_plan_id(mut self, id: usize) -> Self {
        self.plan_id = Some(id);
        self
    }

    pub fn with_node_id(mut self, id: usize) -> Self {
        self.node_id = Some(id);
        self
    }
}

impl Window {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec!["Window:".to_string()];

        for (expr, name) in self.window_functions.iter().zip(self.aliases.iter()) {
            lines.push(format!("  {} as {}", expr, name));
        }

        if !self.window_spec.partition_by.is_empty() {
            let partition_cols = self
                .window_spec
                .partition_by
                .iter()
                .map(|e| e.name().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  Partition by: [{}]", partition_cols));
        }

        if !self.window_spec.order_by.is_empty() {
            let order_cols = self
                .window_spec
                .order_by
                .iter()
                .zip(self.window_spec.descending.iter())
                .zip(self.window_spec.nulls_first.iter())
                .map(|((e, desc), nulls_first)| {
                    format!(
                        "{} {} {}",
                        e.name(),
                        if *desc { "DESC" } else { "ASC" },
                        if *nulls_first {
                            "NULLS FIRST"
                        } else {
                            "NULLS LAST"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  Order by: [{}]", order_cols));
        }

        if let Some(frame) = &self.window_spec.frame {
            let start = match &frame.start {
                daft_dsl::expr::window::WindowBoundary::UnboundedPreceding => {
                    "UNBOUNDED PRECEDING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::UnboundedFollowing => {
                    "UNBOUNDED FOLLOWING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::Offset(n) => match n.cmp(&0) {
                    std::cmp::Ordering::Equal => "CURRENT ROW".to_string(),
                    std::cmp::Ordering::Less => format!("{} PRECEDING", n.abs()),
                    std::cmp::Ordering::Greater => format!("{} FOLLOWING", n),
                },
                daft_dsl::expr::window::WindowBoundary::RangeOffset(n) => {
                    format!("RANGE {}", n)
                }
            };

            let end = match &frame.end {
                daft_dsl::expr::window::WindowBoundary::UnboundedPreceding => {
                    "UNBOUNDED PRECEDING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::UnboundedFollowing => {
                    "UNBOUNDED FOLLOWING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::Offset(n) => match n.cmp(&0) {
                    std::cmp::Ordering::Equal => "CURRENT ROW".to_string(),
                    std::cmp::Ordering::Less => format!("{} PRECEDING", n.abs()),
                    std::cmp::Ordering::Greater => format!("{} FOLLOWING", n),
                },
                daft_dsl::expr::window::WindowBoundary::RangeOffset(n) => {
                    format!("RANGE {}", n)
                }
            };

            lines.push(format!("  Frame: BETWEEN {} AND {}", start, end));
        }

        if self.window_spec.min_periods != 1 {
            lines.push(format!("  Min periods: {}", self.window_spec.min_periods));
        }

        if let StatsState::Materialized(stats) = &self.stats_state {
            lines.push(format!("Stats = {}", stats));
        }

        lines
    }
}
