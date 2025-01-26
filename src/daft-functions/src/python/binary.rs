use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

use crate::binary::{
    concat::binary_concat as concat_fn, length::binary_length as length_fn,
    slice::binary_slice as slice_fn,
};

simple_python_wrapper!(binary_length, length_fn, [input: PyExpr]);
simple_python_wrapper!(binary_concat, concat_fn, [left: PyExpr, right: PyExpr]);
simple_python_wrapper!(binary_slice, slice_fn, [input: PyExpr, start: PyExpr, length: PyExpr]);
