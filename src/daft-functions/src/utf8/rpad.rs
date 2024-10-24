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
pub struct Utf8Rpad {}

#[typetag::serde]
impl ScalarUDF for Utf8Rpad {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "rpad"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, length, pad] => {
                let data = data.to_field(schema)?;
                let length = length.to_field(schema)?;
                let pad = pad.to_field(schema)?;
                if data.dtype == DataType::Utf8
                    && length.dtype.is_integer()
                    && pad.dtype == DataType::Utf8
                {
                    Ok(Field::new(data.name, DataType::Utf8))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to rpad to be utf8, integer and utf8, but received {}, {}, and {}", data.dtype, length.dtype, pad.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, length, pad] => data.utf8_rpad(length, pad),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_rpad(input: ExprRef, length: ExprRef, pad: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Rpad {}, vec![input, length, pad]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_rpad")]
pub fn py_utf8_rpad(expr: PyExpr, length: PyExpr, pad: PyExpr) -> PyResult<PyExpr> {
    Ok(utf8_rpad(expr.into(), length.into(), pad.into()).into())
}
