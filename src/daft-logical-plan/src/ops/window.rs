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
    /// Create a new Window operator.
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        window_functions: Vec<ExprRef>,
        window_spec: WindowSpec,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Clone the input schema fields
        let mut fields = input_schema.fields.clone();

        // Add fields for window function
        for expr in &window_functions {
            fields.insert(expr.name().to_string(), expr.to_field(&input_schema)?);
        }

        // Create a new schema with all fields
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
        vec![format!("Window: {}", self.window_functions.len())]
    }
}
