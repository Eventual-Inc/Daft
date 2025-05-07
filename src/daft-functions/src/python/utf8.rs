use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(utf8_upper, crate::utf8::upper, [input: PyExpr]);
