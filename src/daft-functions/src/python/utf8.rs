use daft_core::array::ops::Utf8NormalizeOptions;
use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(utf8_capitalize, crate::utf8::capitalize, [input: PyExpr]);
simple_python_wrapper!(utf8_contains, crate::utf8::contains, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_endswith, crate::utf8::endswith, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_extract, crate::utf8::extract, [input: PyExpr, pattern: PyExpr, index: usize]);
simple_python_wrapper!(utf8_extract_all, crate::utf8::extract_all, [input: PyExpr, pattern: PyExpr, index: usize]);
simple_python_wrapper!(utf8_find, crate::utf8::find, [input: PyExpr, substr: PyExpr]);
simple_python_wrapper!(utf8_ilike, crate::utf8::ilike, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_left, crate::utf8::left, [input: PyExpr, nchars: PyExpr]);
simple_python_wrapper!(utf8_length, crate::utf8::length, [input: PyExpr]);
simple_python_wrapper!(utf8_length_bytes, crate::utf8::length_bytes, [input: PyExpr]);
simple_python_wrapper!(utf8_like, crate::utf8::like, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_lower, crate::utf8::lower, [input: PyExpr]);
simple_python_wrapper!(utf8_lpad, crate::utf8::lpad, [input: PyExpr, length: PyExpr, pad: PyExpr]);
simple_python_wrapper!(utf8_lstrip, crate::utf8::lstrip, [input: PyExpr]);
simple_python_wrapper!(utf8_match, crate::utf8::match_, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_repeat, crate::utf8::repeat, [input: PyExpr, ntimes: PyExpr]);
simple_python_wrapper!(utf8_replace, crate::utf8::replace, [input: PyExpr, pattern: PyExpr, replacement: PyExpr, regex: bool]);
simple_python_wrapper!(utf8_reverse, crate::utf8::reverse, [input: PyExpr]);
simple_python_wrapper!(utf8_right, crate::utf8::right, [input: PyExpr, nchars: PyExpr]);
simple_python_wrapper!(utf8_rpad, crate::utf8::rpad, [input: PyExpr, length: PyExpr, pad: PyExpr]);
simple_python_wrapper!(utf8_rstrip, crate::utf8::rstrip, [input: PyExpr]);
simple_python_wrapper!(utf8_split, crate::utf8::split, [input: PyExpr, pattern: PyExpr, regex: bool]);
simple_python_wrapper!(utf8_startswith, crate::utf8::startswith, [input: PyExpr, pattern: PyExpr]);
simple_python_wrapper!(utf8_substr, crate::utf8::substr, [input: PyExpr, start: PyExpr, length: PyExpr]);
simple_python_wrapper!(utf8_upper, crate::utf8::upper, [input: PyExpr]);

#[pyfunction]
pub fn utf8_normalize(
    expr: PyExpr,
    remove_punct: bool,
    lowercase: bool,
    nfd_unicode: bool,
    white_space: bool,
) -> PyResult<PyExpr> {
    Ok(crate::utf8::normalize(
        expr.into(),
        Utf8NormalizeOptions {
            remove_punct,
            lowercase,
            nfd_unicode,
            white_space,
        },
    )
    .into())
}

#[pyfunction]
pub fn utf8_to_date(expr: PyExpr, format: &str) -> PyResult<PyExpr> {
    Ok(crate::utf8::to_date(expr.into(), format).into())
}

#[pyfunction(signature = (expr, format, timezone=None))]
pub fn utf8_to_datetime(expr: PyExpr, format: &str, timezone: Option<&str>) -> PyResult<PyExpr> {
    Ok(crate::utf8::to_datetime(expr.into(), format, timezone).into())
}
