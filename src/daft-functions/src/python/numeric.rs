use daft_dsl::python::PyExpr;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

simple_python_wrapper!(abs, crate::numeric::abs::abs, [expr: PyExpr]);
simple_python_wrapper!(cbrt, crate::numeric::cbrt::cbrt, [expr: PyExpr]);
simple_python_wrapper!(ceil, crate::numeric::ceil::ceil, [expr: PyExpr]);
simple_python_wrapper!(clip, crate::numeric::clip::clip, [expr: PyExpr, min: PyExpr, max: PyExpr]);
simple_python_wrapper!(exp, crate::numeric::exp::exp, [expr: PyExpr]);
simple_python_wrapper!(floor, crate::numeric::floor::floor, [expr: PyExpr]);
simple_python_wrapper!(sign, crate::numeric::sign::sign, [expr: PyExpr]);
simple_python_wrapper!(sqrt, crate::numeric::sqrt::sqrt, [expr: PyExpr]);
simple_python_wrapper!(log2, crate::numeric::log::log2, [expr: PyExpr]);
simple_python_wrapper!(log10, crate::numeric::log::log10, [expr: PyExpr]);
simple_python_wrapper!(log, crate::numeric::log::log, [expr: PyExpr, base: f64]);
simple_python_wrapper!(ln, crate::numeric::log::ln, [expr: PyExpr]);
simple_python_wrapper!(sin, crate::numeric::trigonometry::sin, [expr: PyExpr]);
simple_python_wrapper!(cos, crate::numeric::trigonometry::cos, [expr: PyExpr]);
simple_python_wrapper!(tan, crate::numeric::trigonometry::tan, [expr: PyExpr]);
simple_python_wrapper!(cot, crate::numeric::trigonometry::cot, [expr: PyExpr]);
simple_python_wrapper!(arcsin, crate::numeric::trigonometry::arcsin, [expr: PyExpr]);
simple_python_wrapper!(arccos, crate::numeric::trigonometry::arccos, [expr: PyExpr]);
simple_python_wrapper!(arctan, crate::numeric::trigonometry::arctan, [expr: PyExpr]);
simple_python_wrapper!(radians, crate::numeric::trigonometry::radians, [expr: PyExpr]);
simple_python_wrapper!(degrees, crate::numeric::trigonometry::degrees, [expr: PyExpr]);
simple_python_wrapper!(arctanh, crate::numeric::trigonometry::arctanh, [expr: PyExpr]);
simple_python_wrapper!(arccosh, crate::numeric::trigonometry::arccosh, [expr: PyExpr]);
simple_python_wrapper!(arcsinh, crate::numeric::trigonometry::arcsinh, [expr: PyExpr]);
simple_python_wrapper!(arctan2, crate::numeric::trigonometry::atan2, [x: PyExpr, y: PyExpr]);

#[pyfunction]
pub fn round(expr: PyExpr, decimal: i32) -> PyResult<PyExpr> {
    if decimal < 0 {
        return Err(PyValueError::new_err(format!(
            "decimal can not be negative: {decimal}"
        )));
    }
    Ok(crate::numeric::round::round(expr.into(), decimal).into())
}
