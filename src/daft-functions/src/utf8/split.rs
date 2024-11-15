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
pub struct Utf8Split {
    pub regex: bool,
}

#[typetag::serde]
impl ScalarUDF for Utf8Split {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "split"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::List(Box::new(DataType::Utf8))))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to split to be utf8, but received {data_field} and {pattern_field}",
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
            [data, pattern] => data.utf8_split(pattern, self.regex),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_split(input: ExprRef, pattern: ExprRef, regex: bool) -> ExprRef {
    ScalarFunction::new(Utf8Split { regex }, vec![input, pattern]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_split")]
pub fn py_utf8_split(expr: PyExpr, pattern: PyExpr, regex: bool) -> PyResult<PyExpr> {
    Ok(utf8_split(expr.into(), pattern.into(), regex).into())
}
