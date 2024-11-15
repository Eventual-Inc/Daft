use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::infer_timeunit_from_format_string,
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Utf8ToDatetime {
    pub format: String,
    pub timezone: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for Utf8ToDatetime {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "to_datetime"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => {
                        let timeunit = infer_timeunit_from_format_string(&self.format);
                        Ok(Field::new(
                            data_field.name,
                            DataType::Timestamp(timeunit, self.timezone.clone()),
                        ))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to to_datetime to be utf8, but received {data_field}",
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
            [data] => data.utf8_to_datetime(&self.format, self.timezone.as_deref()),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_to_datetime<S: Into<String>>(
    input: ExprRef,
    format: S,
    timezone: Option<S>,
) -> ExprRef {
    ScalarFunction::new(
        Utf8ToDatetime {
            format: format.into(),
            timezone: timezone.map(|s| s.into()),
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
#[pyo3(name = "utf8_to_datetime")]
pub fn py_utf8_to_datetime(expr: PyExpr, format: &str, timezone: Option<&str>) -> PyResult<PyExpr> {
    Ok(utf8_to_datetime::<&str>(expr.into(), format, timezone).into())
}
