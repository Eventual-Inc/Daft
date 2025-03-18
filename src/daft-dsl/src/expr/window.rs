use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, prelude::*};
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    expr::Expr,
    functions::{FunctionEvaluator, FunctionExpr},
};

#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
/// Represents a window frame boundary
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WindowBoundary {
    /// Represents UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING
    Unbounded(UnboundedDirection),
    /// Represents a row offset:
    /// - 0 for CURRENT ROW
    /// - Negative for PRECEDING
    /// - Positive for FOLLOWING
    Offset(i64),
}

#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq))]
/// Direction for unbounded boundaries
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum UnboundedDirection {
    /// Unbounded Preceding
    Preceding,
    /// Unbounded Following
    Following,
}

impl WindowBoundary {
    /// Helper to create an UNBOUNDED PRECEDING boundary
    pub fn unbounded_preceding() -> Self {
        Self::Unbounded(UnboundedDirection::Preceding)
    }

    /// Helper to create an UNBOUNDED FOLLOWING boundary
    pub fn unbounded_following() -> Self {
        Self::Unbounded(UnboundedDirection::Following)
    }

    /// Helper to create a CURRENT ROW boundary
    pub fn current_row() -> Self {
        Self::Offset(0)
    }

    /// Helper to create a PRECEDING boundary with a positive number of rows
    pub fn preceding(n: u64) -> Self {
        assert!(n > 0, "PRECEDING value must be positive");
        Self::Offset(-(n as i64))
    }

    /// Helper to create a FOLLOWING boundary with a positive number of rows
    pub fn following(n: u64) -> Self {
        assert!(n > 0, "FOLLOWING value must be positive");
        Self::Offset(n as i64)
    }
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
    pub start: WindowBoundary,
    /// End boundary of window frame
    pub end: WindowBoundary,
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
    /// Minimum number of observations required to produce a value
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

    pub fn with_partition_by(&self, exprs: Vec<Arc<Expr>>) -> Self {
        let mut new_spec = self.clone();
        new_spec.partition_by = exprs;
        new_spec
    }

    pub fn with_order_by(&self, exprs: Vec<Arc<Expr>>, ascending: Vec<bool>) -> Self {
        assert_eq!(
            exprs.len(),
            ascending.len(),
            "Order by expressions and ascending flags must have same length"
        );
        let mut new_spec = self.clone();
        new_spec.order_by = exprs;
        new_spec.ascending = ascending;
        new_spec
    }

    pub fn with_frame(&self, frame: WindowFrame) -> Self {
        let mut new_spec = self.clone();
        new_spec.frame = Some(frame);
        new_spec
    }

    pub fn with_min_periods(&self, min_periods: i64) -> Self {
        let mut new_spec = self.clone();
        new_spec.min_periods = min_periods;
        new_spec
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
            "Window functions should be rewritten into a separate plan step by the optimizer. If you're seeing this error, the ExtractWindowFunction optimization rule may not have been applied.".to_string(),
        ))
    }
}
