use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Utf8Normalize {
    pub opts: Utf8NormalizeOptions,
}

#[typetag::serde]
impl ScalarUDF for Utf8Normalize {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "normalize"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to normalize to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => data.utf8_normalize(self.opts),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_normalize(input: ExprRef, opts: Utf8NormalizeOptions) -> ExprRef {
    ScalarFunction::new(Utf8Normalize { opts }, vec![input]).into()
}

use daft_core::array::ops::Utf8NormalizeOptions;
#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_normalize")]
pub fn py_utf8_normalize(
    expr: PyExpr,
    remove_punct: bool,
    lowercase: bool,
    nfd_unicode: bool,
    white_space: bool,
) -> PyResult<PyExpr> {
    Ok(utf8_normalize(
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
