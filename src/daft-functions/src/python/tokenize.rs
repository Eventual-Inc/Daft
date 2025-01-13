use daft_dsl::python::PyExpr;
use daft_io::python::IOConfig;
use pyo3::{pyfunction, PyResult};

#[pyfunction(signature = (
    expr,
    tokens_path,
    use_special_tokens,
    io_config=None,
    pattern=None,
    special_tokens=None
))]
pub fn tokenize_encode(
    expr: PyExpr,
    tokens_path: &str,
    use_special_tokens: bool,
    io_config: Option<IOConfig>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
) -> PyResult<PyExpr> {
    Ok(crate::tokenize::tokenize_encode(
        expr.into(),
        tokens_path,
        io_config.map(|config| config.config),
        pattern,
        special_tokens,
        use_special_tokens,
    )
    .into())
}

#[pyfunction(signature = (
    expr,
    tokens_path,
    io_config=None,
    pattern=None,
    special_tokens=None
))]
pub fn tokenize_decode(
    expr: PyExpr,
    tokens_path: &str,
    io_config: Option<IOConfig>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
) -> PyResult<PyExpr> {
    Ok(crate::tokenize::tokenize_decode(
        expr.into(),
        tokens_path,
        io_config.map(|config| config.config),
        pattern,
        special_tokens,
    )
    .into())
}
