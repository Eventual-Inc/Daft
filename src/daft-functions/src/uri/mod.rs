mod download;
mod upload;

use common_io_config::IOConfig;
use daft_dsl::{functions::ScalarFunction, ExprRef};
use download::DownloadFunction;
use upload::UploadFunction;

pub fn download(
    input: ExprRef,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Option<IOConfig>,
) -> ExprRef {
    ScalarFunction::new(
        DownloadFunction {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config: config.unwrap_or_default().into(),
        },
        vec![input],
    )
    .into()
}

pub fn upload(
    input: ExprRef,
    location: &str,
    max_connections: usize,
    multi_thread: bool,
    config: Option<IOConfig>,
) -> ExprRef {
    ScalarFunction::new(
        UploadFunction {
            location: location.to_string(),
            max_connections,
            multi_thread,
            config: config.unwrap_or_default().into(),
        },
        vec![input],
    )
    .into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use daft_io::python::IOConfig as PyIOConfig;
    use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

    #[pyfunction]
    pub fn url_download(
        expr: PyExpr,
        max_connections: i64,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: PyIOConfig,
    ) -> PyResult<PyExpr> {
        if max_connections <= 0 {
            return Err(PyValueError::new_err(format!(
                "max_connections must be positive and non_zero: {max_connections}"
            )));
        }

        Ok(super::download(
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
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyExpr> {
        if max_connections <= 0 {
            return Err(PyValueError::new_err(format!(
                "max_connections must be positive and non_zero: {max_connections}"
            )));
        }
        Ok(super::upload(
            expr.into(),
            folder_location,
            max_connections as usize,
            multi_thread,
            io_config.map(|io_config| io_config.config),
        )
        .into())
    }
}
