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
pub struct Utf8Repeat {}

#[typetag::serde]
impl ScalarUDF for Utf8Repeat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, ntimes] => match (data.to_field(schema), ntimes.to_field(schema)) {
                (Ok(data_field), Ok(ntimes_field)) => {
                    match (&data_field.dtype, &ntimes_field.dtype) {
                        (DataType::Utf8, dt) if dt.is_integer() => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to repeat to be utf8 and integer, but received {data_field} and {ntimes_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, ntimes] => data.utf8_repeat(ntimes),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_repeat(input: ExprRef, ntimes: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Repeat {}, vec![input, ntimes]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_repeat")]
pub fn py_utf8_repeat(expr: PyExpr, ntimes: PyExpr) -> PyResult<PyExpr> {
    Ok(utf8_repeat(expr.into(), ntimes.into()).into())
}
