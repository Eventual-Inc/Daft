use std::sync::Arc;

use crate::dsl::{self, Expr};
use pyo3::{prelude::*, pyclass::CompareOp};

#[pyfunction]
pub fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(dsl::col(name)))
}

#[pyfunction]
pub fn lit(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(dsl::col(name)))
}

#[pyclass]
pub struct PyExpr {
    expr: dsl::Expr,
}

#[pymethods]
impl PyExpr {
    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Plus, &self.expr, &other.expr).into())
    }

    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        use dsl::{binary_op, Operator};
        match op {
            CompareOp::Lt => Ok(binary_op(Operator::Lt, &self.expr, &other.expr).into()),
            CompareOp::Le => Ok(binary_op(Operator::LtEq, &self.expr, &other.expr).into()),
            CompareOp::Eq => Ok(binary_op(Operator::Eq, &self.expr, &other.expr).into()),
            CompareOp::Ne => Ok(binary_op(Operator::NotEq, &self.expr, &other.expr).into()),
            CompareOp::Gt => Ok(binary_op(Operator::Gt, &self.expr, &other.expr).into()),
            CompareOp::Ge => Ok(binary_op(Operator::GtEq, &self.expr, &other.expr).into()),
        }
    }
}

impl From<dsl::Expr> for PyExpr {
    fn from(value: dsl::Expr) -> Self {
        PyExpr { expr: value }
    }
}

impl From<PyExpr> for dsl::Expr {
    fn from(item: PyExpr) -> Self {
        item.expr
    }
}
