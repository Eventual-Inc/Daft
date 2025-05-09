use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{format_string_has_offset, infer_timeunit_from_format_string},
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
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
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

                        let timezone = if let Some(tz) = &self.timezone {
                            Some(tz.clone())
                        } else if format_string_has_offset(&self.format) {
                            // if it has an offset, we coerce it to UTC. This is consistent with other engines (duckdb, polars)
                            Some("UTC".to_string())
                        } else {
                            None
                        };

                        Ok(Field::new(
                            data_field.name,
                            DataType::Timestamp(timeunit, timezone),
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

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
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
