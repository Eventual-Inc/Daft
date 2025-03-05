use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, prelude::*};
use serde::{Deserialize, Serialize};

use crate::{
    expr::Expr,
    functions::{FunctionEvaluator, FunctionExpr},
};

/// Represents a window frame boundary
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WindowFrameBoundary {
    /// Represents UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// Represents UNBOUNDED FOLLOWING
    UnboundedFollowing,
    /// Represents CURRENT ROW
    CurrentRow,
    /// Represents N PRECEDING
    Preceding(i64),
    /// Represents N FOLLOWING
    Following(i64),
}

/// Represents the type of window frame (ROWS or RANGE)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WindowFrameType {
    /// Row-based window frame
    Rows,
    /// Range-based window frame
    Range,
}

/// Represents a window frame specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct WindowFrame {
    /// Type of window frame (ROWS or RANGE)
    pub frame_type: WindowFrameType,
    /// Start boundary of window frame
    pub start: WindowFrameBoundary,
    /// End boundary of window frame
    pub end: WindowFrameBoundary,
}

/// Represents a window specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct WindowSpec {
    /// Partition by expressions
    pub partition_by: Vec<Arc<Expr>>,
    /// Order by expressions
    pub order_by: Vec<Arc<Expr>>,
    /// Whether each order by expression is ascending
    pub ascending: Vec<bool>,
    /// Window frame specification
    pub frame: Option<WindowFrame>,
    /// Minimum number of rows required to compute a result
    pub min_periods: i64,
}

impl WindowSpec {
    pub fn new() -> Self {
        Self {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            ascending: Vec::new(),
            frame: None,
            min_periods: 1,
        }
    }

    pub fn with_partition_by(mut self, exprs: Vec<Arc<Expr>>) -> Self {
        self.partition_by = exprs;
        self
    }

    pub fn with_order_by(mut self, exprs: Vec<Arc<Expr>>, ascending: Vec<bool>) -> Self {
        assert_eq!(
            exprs.len(),
            ascending.len(),
            "Order by expressions and ascending flags must have same length"
        );
        self.order_by = exprs;
        self.ascending = ascending;
        self
    }

    pub fn with_frame(mut self, frame: WindowFrame) -> Self {
        self.frame = Some(frame);
        self
    }

    pub fn with_min_periods(mut self, min_periods: i64) -> Self {
        self.min_periods = min_periods;
        self
    }
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a window function expression
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct WindowFunction {
    /// The expression to apply the window function to
    pub expr: Arc<Expr>,
    /// The window specification
    pub window_spec: WindowSpec,
}

impl WindowFunction {
    pub fn new(expr: Expr, window_spec: WindowSpec) -> Self {
        Self {
            expr: Arc::new(expr),
            window_spec,
        }
    }

    pub fn data_type(&self) -> DaftResult<DataType> {
        // For basic window functions like sum, the data type is the same as the input expression
        // TODO: For more complex window functions (rank, dense_rank, etc.), implement specific type inference
        // based on the window function type

        // Get the data type from the input expression by using to_field with an empty schema
        let schema = Schema::empty();
        let field = self.expr.to_field(&schema)?;
        Ok(field.dtype)
    }
    
    /// Get the name of the window function from its underlying expression
    pub fn name(&self) -> &'static str {
        // Return a default name in case the expression doesn't have a name
        // This prevents the Option::unwrap() None panic
        "window_function"
    }
}

impl FunctionEvaluator for WindowFunction {
    fn fn_name(&self) -> &'static str {
        "window"
    }

    fn to_field(
        &self,
        _inputs: &[crate::ExprRef],
        schema: &Schema,
        _expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        // The output field has the same name and type as the input expression
        self.expr.to_field(schema)
    }

    fn evaluate(&self, _inputs: &[Series], _expr: &FunctionExpr) -> DaftResult<Series> {
        Err(DaftError::NotImplemented(
            "Window functions should be rewritten into a separate plan step by the optimizer. If you're seeing this error, the DetectWindowFunctions optimization rule may not have been applied.".to_string(),
        ))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowExpr {
    /// The window frame specification
    pub frame: WindowFrame,
    /// The data type of the window expression
    pub data_type: DataType,
}
