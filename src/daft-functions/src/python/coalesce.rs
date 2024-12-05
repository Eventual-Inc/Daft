use daft_dsl::python::PyExpr;
use pyo3::pyfunction;

#[pyfunction]
pub fn coalesce(exprs: Vec<PyExpr>) -> PyExpr {
    crate::coalesce::coalesce(exprs.into_iter().map(|expr| expr.into()).collect()).into()
}
