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
pub struct Utf8ToDate {
    pub format: String,
}

#[typetag::serde]
impl ScalarUDF for Utf8ToDate {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "to_date"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Date)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to to_date to be utf8, but received {data_field}",
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
            [data] => data.utf8_to_date(&self.format),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_to_date<S: Into<String>>(input: ExprRef, format: S) -> ExprRef {
    ScalarFunction::new(
        Utf8ToDate {
            format: format.into(),
        },
        vec![input],
    )
    .into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "utf8_to_date")]
pub fn py_utf8_to_date(expr: PyExpr, format: &str) -> PyResult<PyExpr> {
    Ok(utf8_to_date::<&str>(expr.into(), format).into())
}
