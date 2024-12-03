use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(fill_nan, crate::float::fill_nan, [expr: PyExpr, fill_value: PyExpr]);
simple_python_wrapper!(is_inf, crate::float::is_inf, [expr: PyExpr]);
simple_python_wrapper!(is_nan, crate::float::is_nan, [expr: PyExpr]);
simple_python_wrapper!(not_nan, crate::float::not_nan, [expr: PyExpr]);
