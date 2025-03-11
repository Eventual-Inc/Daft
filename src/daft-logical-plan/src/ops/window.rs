use std::sync::Arc;

use common_error::DaftError;
use daft_core::prelude::*;
use daft_dsl::{expr::window::WindowFrame, ExprRef};

use crate::{
    logical_plan::{Error, LogicalPlan, Result},
    stats::StatsState,
};

/// Window operator for computing window functions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    /// The input plan.
    pub input: Arc<LogicalPlan>,
    /// The window functions to compute.
    pub window_functions: Vec<ExprRef>,
    /// The columns to partition by.
    pub partition_by: Vec<ExprRef>,
    /// The columns to order by.
    pub order_by: Vec<ExprRef>,
    /// The ascending flags for the order by columns.
    pub ascending: Vec<bool>,
    /// The window frame.
    pub frame: Option<WindowFrame>,
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
        partition_by: Vec<ExprRef>,
        order_by: Vec<ExprRef>,
        ascending: Vec<bool>,
        frame: Option<WindowFrame>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Clone the input schema fields
        let mut fields = input_schema.fields.clone();

        // Add fields for window function expressions with auto-generated names (window_0, window_1, etc.)
        for (i, expr) in window_functions.iter().enumerate() {
            let window_col_name = format!("window_{}", i);
            let expr_type = expr.get_type(&input_schema)?;
            let field = Field::new(&window_col_name, expr_type);
            fields.insert(window_col_name, field);
        }

        // Create a new schema with all fields
        let schema = Arc::new(Schema::new(fields.values().cloned().collect())?);

        Ok(Self {
            plan_id: None,
            input,
            window_functions,
            partition_by,
            order_by,
            ascending,
            frame,
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
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            ascending: self.ascending.clone(),
            frame: self.frame.clone(),
            schema: self.schema.clone(),
            stats_state: self.stats_state.clone(),
        })
    }
}

impl Window {
    /// Get the children of this operator.
    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        vec![self.input.clone()]
    }

    pub(crate) fn _with_children(
        &self,
        children: Vec<Arc<LogicalPlan>>,
    ) -> Result<Arc<LogicalPlan>> {
        if children.len() != 1 {
            return Err(Error::CreationError {
                source: DaftError::InternalError(format!(
                    "Window requires exactly one child, got {}",
                    children.len()
                )),
            });
        }

        Ok(Arc::new(LogicalPlan::Window(Self {
            plan_id: self.plan_id,
            input: children[0].clone(),
            window_functions: self.window_functions.clone(),
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            ascending: self.ascending.clone(),
            frame: self.frame.clone(),
            schema: self.schema.clone(),
            stats_state: self.stats_state.clone(),
        })))
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn stats(&self) -> &StatsState {
        &self.stats_state
    }

    pub fn plan_id(&self) -> &Option<usize> {
        &self.plan_id
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Window: {}", self.window_functions.len())]
    }
}
