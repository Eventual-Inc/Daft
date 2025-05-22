use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

use crate::binary;

simple_python_wrapper!(binary_length, binary::binary_length, [input: PyExpr]);
simple_python_wrapper!(binary_slice, binary::binary_slice, [input: PyExpr, start: PyExpr, length: PyExpr]);
