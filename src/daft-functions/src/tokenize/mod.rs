use daft_dsl::{functions::ScalarFunction, ExprRef};
use function::{TokenizeDecodeFunction, TokenizeEncodeFunction};

mod array;
mod bpe;
mod function;
mod series;

pub fn tokenize_encode(data: ExprRef, tokens_path: &str) -> ExprRef {
    ScalarFunction::new(
        TokenizeEncodeFunction {
            tokens_path: tokens_path.to_string(),
        },
        vec![data],
    )
    .into()
}

pub fn tokenize_decode(data: ExprRef, tokens_path: &str) -> ExprRef {
    ScalarFunction::new(
        TokenizeDecodeFunction {
            tokens_path: tokens_path.to_string(),
        },
        vec![data],
    )
    .into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    use super::{tokenize_decode as rust_decode, tokenize_encode as rust_encode};

    #[pyfunction]
    pub fn tokenize_encode(expr: PyExpr, tokens_path: &str) -> PyResult<PyExpr> {
        Ok(rust_encode(expr.into(), tokens_path).into())
    }

    #[pyfunction]
    pub fn tokenize_decode(expr: PyExpr, tokens_path: &str) -> PyResult<PyExpr> {
        Ok(rust_decode(expr.into(), tokens_path).into())
    }
}
