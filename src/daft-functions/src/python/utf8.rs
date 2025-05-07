use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(utf8_substr, crate::utf8::substr, [input: PyExpr, start: PyExpr, length: PyExpr]);
simple_python_wrapper!(utf8_upper, crate::utf8::upper, [input: PyExpr]);

#[pyfunction]
pub fn utf8_to_date(expr: PyExpr, format: &str) -> PyResult<PyExpr> {
    Ok(crate::utf8::to_date(expr.into(), format).into())
}

#[pyfunction(signature = (expr, format, timezone=None))]
pub fn utf8_to_datetime(expr: PyExpr, format: &str, timezone: Option<&str>) -> PyResult<PyExpr> {
    Ok(crate::utf8::to_datetime(expr.into(), format, timezone).into())
}
