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
pub struct Utf8Left {}

#[typetag::serde]
impl ScalarUDF for Utf8Left {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "left"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, nchars] => match (data.to_field(schema), nchars.to_field(schema)) {
                (Ok(data_field), Ok(nchars_field)) => {
                    match (&data_field.dtype, &nchars_field.dtype) {
                        (DataType::Utf8, dt) if dt.is_integer() => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to left to be utf8 and integer, but received {data_field} and {nchars_field}",
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
            [data, nchars] => data.utf8_left(nchars),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_left(input: ExprRef, nchars: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Left {}, vec![input, nchars]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_left")]
pub fn py_utf8_left(expr: PyExpr, nchars: PyExpr) -> PyResult<PyExpr> {
    Ok(utf8_left(expr.into(), nchars.into()).into())
}
