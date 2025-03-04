#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;

#[cfg(feature = "python")]
pub fn rank(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn dense_rank(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn row_number(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn percent_rank(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn ntile(expr: PyExpr, _n: i64) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn first_value(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn last_value(expr: PyExpr) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn nth_value(expr: PyExpr, _n: i64) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn lag(expr: PyExpr, _offset: i64, _default: Option<PyExpr>) -> PyExpr {
    expr
}

#[cfg(feature = "python")]
pub fn lead(expr: PyExpr, _offset: i64, _default: Option<PyExpr>) -> PyExpr {
    expr
}
