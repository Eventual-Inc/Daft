use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Contains;

#[typetag::serde]
impl ScalarUDF for Contains {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        series_contains(input, pattern)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "contains expects 2 arguments");

        let input = inputs.required((0, "input"))?.to_field(schema)?;

        ensure!(input.dtype == DataType::Utf8, TypeError: "input must be of type Utf8");

        let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
        ensure!(
            pattern.dtype == DataType::Utf8,
            TypeError: "pattern must be of type Utf8"
        );
        Ok(Field::new(input.name, DataType::Boolean))
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Boolean))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to contains to be utf8, but received {data_field} and {pattern_field}",
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

    fn name(&self) -> &'static str {
        "contains"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["utf8_contains"]
    }

    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether each string contains the specified pattern."
    }
}

pub fn contains(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(Contains, vec![input, pattern]).into()
}
pub fn series_contains(s: &Series, pattern: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern| {
            arr.binary_broadcasted_compare(
                pattern,
                |data: &str, pat: &str| Ok(data.contains(pat)),
                "contains",
            )
            .map(IntoSeries::into_series)
        })
    })
}
