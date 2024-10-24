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
pub struct Utf8Find {}

#[typetag::serde]
impl ScalarUDF for Utf8Find {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "find"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, substr] => match (data.to_field(schema), substr.to_field(schema)) {
                (Ok(data_field), Ok(substr_field)) => {
                    match (&data_field.dtype, &substr_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Int64))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to find to be utf8 and utf8, but received {data_field} and {substr_field}",
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
            [data, substr] => data.utf8_find(substr),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_find(input: ExprRef, substr: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Find {}, vec![input, substr]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_find")]
pub fn py_utf8_find(expr: PyExpr, substr: PyExpr) -> PyResult<PyExpr> {
    Ok(utf8_find(expr.into(), substr.into()).into())
}
