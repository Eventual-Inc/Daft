use daft_dsl::{functions::ScalarFunction, ExprRef};
use daft_io::IOConfig;
use decode::TokenizeDecodeFunction;
use encode::TokenizeEncodeFunction;

mod bpe;
mod decode;
mod encode;

pub fn tokenize_encode(data: ExprRef, tokens_path: &str, io_config: Option<IOConfig>) -> ExprRef {
    ScalarFunction::new(
        TokenizeEncodeFunction {
            tokens_path: tokens_path.to_string(),
            io_config: io_config.unwrap_or_default().into(),
        },
        vec![data],
    )
    .into()
}

pub fn tokenize_decode(data: ExprRef, tokens_path: &str, io_config: Option<IOConfig>) -> ExprRef {
    ScalarFunction::new(
        TokenizeDecodeFunction {
            tokens_path: tokens_path.to_string(),
            io_config: io_config.unwrap_or_default().into(),
        },
        vec![data],
    )
    .into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use daft_io::python::IOConfig as PyIOConfig;
    use pyo3::{pyfunction, PyResult};

    use super::{tokenize_decode as rust_decode, tokenize_encode as rust_encode};

    #[pyfunction]
    pub fn tokenize_encode(
        expr: PyExpr,
        tokens_path: &str,
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyExpr> {
        Ok(rust_encode(expr.into(), tokens_path, io_config.map(|x| x.config)).into())
    }

    #[pyfunction]
    pub fn tokenize_decode(
        expr: PyExpr,
        tokens_path: &str,
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyExpr> {
        Ok(rust_decode(expr.into(), tokens_path, io_config.map(|x| x.config)).into())
    }
}
