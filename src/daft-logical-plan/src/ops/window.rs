use std::sync::Arc;

use daft_core::prelude::*;
use daft_dsl::{expr::window::WindowSpec, ExprRef};

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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    /// The input plan.
    pub input: Arc<LogicalPlan>,
    /// The window functions to compute.
    pub window_functions: Vec<ExprRef>,
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
        window_functions: Vec<ExprRef>,
        window_spec: WindowSpec,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let mut fields = input_schema.fields.clone();

        for expr in &window_functions {
            fields.insert(expr.name().to_string(), expr.to_field(&input_schema)?);
        }

        let schema = Arc::new(Schema::new(fields.values().cloned().collect())?);

        Ok(Self {
            plan_id: None,
            input,
            window_functions,
            window_spec,
            schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_window_functions(mut self, window_functions: Vec<ExprRef>) -> Self {
        self.window_functions = window_functions;
        self
    }

    pub fn with_materialized_stats(mut self) -> Self {
        // For now, just use the input's stats as an approximation
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn with_plan_id(&self, id: Option<usize>) -> LogicalPlan {
        LogicalPlan::Window(Self {
            plan_id: id,
            input: self.input.clone(),
            window_functions: self.window_functions.clone(),
            window_spec: self.window_spec.clone(),
            schema: self.schema.clone(),
            stats_state: self.stats_state.clone(),
        })
    }
}

impl Window {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![format!("Window: {}", self.window_functions.len())];

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
                .zip(self.window_spec.ascending.iter())
                .map(|(e, asc)| format!("{} {}", e.name(), if *asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("  Order by: [{}]", order_cols));
        }

        if let Some(frame) = &self.window_spec.frame {
            let frame_type = match frame.frame_type {
                daft_dsl::expr::window::WindowFrameType::Rows => "ROWS",
                daft_dsl::expr::window::WindowFrameType::Range => "RANGE",
            };

            let start = match &frame.start {
                daft_dsl::expr::window::WindowBoundary::UnboundedPreceding() => {
                    "UNBOUNDED PRECEDING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::UnboundedFollowing() => {
                    "UNBOUNDED FOLLOWING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::Offset(n) => match n.cmp(&0) {
                    std::cmp::Ordering::Equal => "CURRENT ROW".to_string(),
                    std::cmp::Ordering::Less => format!("{} PRECEDING", n.abs()),
                    std::cmp::Ordering::Greater => format!("{} FOLLOWING", n),
                },
            };

            let end = match &frame.end {
                daft_dsl::expr::window::WindowBoundary::UnboundedPreceding() => {
                    "UNBOUNDED PRECEDING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::UnboundedFollowing() => {
                    "UNBOUNDED FOLLOWING".to_string()
                }
                daft_dsl::expr::window::WindowBoundary::Offset(n) => match n.cmp(&0) {
                    std::cmp::Ordering::Equal => "CURRENT ROW".to_string(),
                    std::cmp::Ordering::Less => format!("{} PRECEDING", n.abs()),
                    std::cmp::Ordering::Greater => format!("{} FOLLOWING", n),
                },
            };

            lines.push(format!(
                "  Frame: {} BETWEEN {} AND {}",
                frame_type, start, end
            ));
        }

        if self.window_spec.min_periods != 1 {
            lines.push(format!("  Min periods: {}", self.window_spec.min_periods));
        }

        lines
    }
}
