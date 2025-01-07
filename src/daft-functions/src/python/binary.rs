use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

use crate::binary::{
    length::binary_length as length_fn,
    concat::binary_concat as concat_fn,
};

#[pyfunction]
pub fn binary_length(input: PyExpr) -> PyResult<PyExpr> {
    Ok(length_fn(input.into()).into())
}

#[pyfunction]
pub fn binary_concat(left: PyExpr, right: PyExpr) -> PyResult<PyExpr> {
    Ok(concat_fn(left.into(), right.into()).into())
}
