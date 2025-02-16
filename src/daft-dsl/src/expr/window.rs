use common_error::DaftResult;
use daft_core::datatypes::DataType;

use crate::expr::Expr;

/// Represents a window frame boundary
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFrameType {
    /// Row-based window frame
    Rows,
    /// Range-based window frame
    Range,
}

/// Represents a window frame specification
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrame {
    /// Type of window frame (ROWS or RANGE)
    pub frame_type: WindowFrameType,
    /// Start boundary of window frame
    pub start: WindowFrameBoundary,
    /// End boundary of window frame
    pub end: WindowFrameBoundary,
}

/// Represents a window specification
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowSpec {
    /// Partition by expressions
    pub partition_by: Vec<Expr>,
    /// Order by expressions
    pub order_by: Vec<Expr>,
    /// Whether each order by expression is ascending
    pub ascending: Vec<bool>,
    /// Window frame specification
    pub frame: Option<WindowFrame>,
}

/// Represents a window function expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFunction {
    /// The expression to apply the window function to
    pub expr: Box<Expr>,
    /// The window specification
    pub window_spec: WindowSpec,
}

impl WindowFunction {
    pub fn new(expr: Expr, window_spec: WindowSpec) -> Self {
        Self {
            expr: Box::new(expr),
            window_spec,
        }
    }

    pub fn data_type(&self) -> DaftResult<DataType> {
        // TODO: Implement data type inference for window functions
        todo!("Implement data type inference for window functions")
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
