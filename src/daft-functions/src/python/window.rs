use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

// Ranking functions
simple_python_wrapper!(rank, crate::window::rank, [expr: PyExpr]);
simple_python_wrapper!(dense_rank, crate::window::dense_rank, [expr: PyExpr]);
simple_python_wrapper!(row_number, crate::window::row_number, [expr: PyExpr]);
simple_python_wrapper!(percent_rank, crate::window::percent_rank, [expr: PyExpr]);
simple_python_wrapper!(ntile, crate::window::ntile, [expr: PyExpr, n: i64]);

// Analytics functions
simple_python_wrapper!(first_value, crate::window::first_value, [expr: PyExpr]);
simple_python_wrapper!(last_value, crate::window::last_value, [expr: PyExpr]);
simple_python_wrapper!(nth_value, crate::window::nth_value, [expr: PyExpr, n: i64]);

// Offset functions with optional default value
#[pyfunction]
#[pyo3(signature = (expr, offset, default=None))]
pub fn lag(expr: PyExpr, offset: i64, default: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(crate::window::lag(expr, offset, default.map(Into::into)))
}

#[pyfunction]
#[pyo3(signature = (expr, offset, default=None))]
pub fn lead(expr: PyExpr, offset: i64, default: Option<PyExpr>) -> PyResult<PyExpr> {
    Ok(crate::window::lead(expr, offset, default.map(Into::into)))
}
