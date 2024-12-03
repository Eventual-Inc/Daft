use daft_dsl::python::PyExpr;
use daft_io::python::IOConfig;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

#[pyfunction]
pub fn url_download(
    expr: PyExpr,
    max_connections: i64,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: IOConfig,
) -> PyResult<PyExpr> {
    if max_connections <= 0 {
        return Err(PyValueError::new_err(format!(
            "max_connections must be positive and non_zero: {max_connections}"
        )));
    }

    Ok(crate::uri::download(
        expr.into(),
        max_connections as usize,
        raise_error_on_failure,
        multi_thread,
        Some(config.config),
    )
    .into())
}

#[pyfunction]
pub fn url_upload(
    expr: PyExpr,
    folder_location: &str,
    max_connections: i64,
    multi_thread: bool,
    io_config: Option<IOConfig>,
) -> PyResult<PyExpr> {
    if max_connections <= 0 {
        return Err(PyValueError::new_err(format!(
            "max_connections must be positive and non_zero: {max_connections}"
        )));
    }
    Ok(crate::uri::upload(
        expr.into(),
        folder_location,
        max_connections as usize,
        multi_thread,
        io_config.map(|io_config| io_config.config),
    )
    .into())
}
