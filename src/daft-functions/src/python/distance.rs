use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(cosine_distance, crate::distance::cosine::cosine_distance, [a: PyExpr, b: PyExpr]);
