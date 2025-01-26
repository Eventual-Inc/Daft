use daft_dsl::python::PyExpr;
use daft_io::python::IOConfig;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

use crate::uri::{self, download::UrlDownloadArgs, upload::UrlUploadArgs};

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
    let args = UrlDownloadArgs::new(
        max_connections as usize,
        raise_error_on_failure,
        multi_thread,
        Some(config.config),
    );
    Ok(uri::download(expr.into(), Some(args)).into())
}

#[pyfunction(signature = (
    expr,
    folder_location,
    max_connections,
    raise_error_on_failure,
    multi_thread,
    is_single_folder,
    io_config=None
))]
pub fn url_upload(
    expr: PyExpr,
    folder_location: PyExpr,
    max_connections: i64,
    raise_error_on_failure: bool,
    multi_thread: bool,
    is_single_folder: bool,
    io_config: Option<IOConfig>,
) -> PyResult<PyExpr> {
    if max_connections <= 0 {
        return Err(PyValueError::new_err(format!(
            "max_connections must be positive and non_zero: {max_connections}"
        )));
    }
    let args = UrlUploadArgs::new(
        max_connections as usize,
        raise_error_on_failure,
        multi_thread,
        is_single_folder,
        io_config.map(|io_config| io_config.config),
    );
    Ok(uri::upload(expr.into(), folder_location.into(), Some(args)).into())
}
