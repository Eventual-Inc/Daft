use std::sync::Arc;

#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::expr::Expr;
#[cfg(feature = "python")]
use crate::python::PyExpr;

/// Represents a window frame boundary
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum WindowBoundary {
    /// Represents UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING
    UnboundedPreceding(),
    UnboundedFollowing(),
    /// Represents a row offset:
    /// - 0 for CURRENT ROW
    /// - Negative for PRECEDING
    /// - Positive for FOLLOWING
    Offset(i64),
}

#[cfg(feature = "python")]
#[pymethods]
impl WindowBoundary {
    #[staticmethod]
    pub fn unbounded_preceding() -> Self {
        Self::UnboundedPreceding()
    }

    #[staticmethod]
    pub fn unbounded_following() -> Self {
        Self::UnboundedFollowing()
    }

    #[staticmethod]
    pub fn offset(n: i64) -> Self {
        Self::Offset(n)
    }
}

/// Represents the type of window frame (ROWS or RANGE)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum WindowFrameType {
    /// Row-based window frame
    Rows,
    /// Range-based window frame
    Range,
}

/// Represents a window frame specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct WindowFrame {
    /// Type of window frame (ROWS or RANGE)
    pub frame_type: WindowFrameType,
    /// Start boundary of window frame
    pub start: WindowBoundary,
    /// End boundary of window frame
    pub end: WindowBoundary,
}

#[cfg(feature = "python")]
#[pymethods]
impl WindowFrame {
    #[new]
    pub fn new(frame_type: WindowFrameType, start: WindowBoundary, end: WindowBoundary) -> Self {
        Self {
            frame_type,
            start,
            end,
        }
    }
}

/// Represents a window specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
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

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            ascending: Vec::new(),
            frame: None,
            min_periods: 1,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl WindowSpec {
    #[staticmethod]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_partition_by(&self, exprs: Vec<PyExpr>) -> Self {
        let mut new_spec = self.clone();
        new_spec.partition_by = exprs.into_iter().map(|e| e.expr).collect();
        new_spec
    }

    pub fn with_order_by(&self, exprs: Vec<PyExpr>, ascending: Vec<bool>) -> Self {
        assert_eq!(
            exprs.len(),
            ascending.len(),
            "Order by expressions and ascending flags must have same length"
        );
        let mut new_spec = self.clone();
        new_spec.order_by = exprs.into_iter().map(|e| e.expr).collect();
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
