use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

use crate::binary::{self, codecs::Codec};

simple_python_wrapper!(binary_length, binary::binary_length, [input: PyExpr]);
simple_python_wrapper!(binary_concat, binary::binary_concat, [left: PyExpr, right: PyExpr]);
simple_python_wrapper!(binary_slice, binary::binary_slice, [input: PyExpr, start: PyExpr, length: PyExpr]);

#[pyfunction]
pub fn encode(input: PyExpr, codec: &str) -> PyResult<PyExpr> {
    Ok(binary::encode::encode(input.expr, Codec::try_from(codec)?).into())
}

#[pyfunction]
pub fn decode(input: PyExpr, codec: &str) -> PyResult<PyExpr> {
    Ok(binary::decode::decode(input.expr, Codec::try_from(codec)?).into())
}

#[pyfunction]
pub fn try_encode(input: PyExpr, codec: &str) -> PyResult<PyExpr> {
    Ok(binary::encode::try_encode(input.expr, Codec::try_from(codec)?).into())
}

#[pyfunction]
pub fn try_decode(input: PyExpr, codec: &str) -> PyResult<PyExpr> {
    Ok(binary::decode::try_decode(input.expr, Codec::try_from(codec)?).into())
}
