use crate::dsl;
use pyo3::prelude::*;

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
    fn __add__(&self, other: &Self) -> Self {
        (&self.expr + &other.expr).into()
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
