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
pub struct StartsWith;

#[typetag::serde]
impl ScalarUDF for StartsWith {
    fn name(&self) -> &'static str {
        "starts_with"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        series_startswith(input, pattern)
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
    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether each string starts with the specified pattern"
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => data.utf8_startswith(pattern),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn startswith(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(StartsWith {}, vec![input, pattern]).into()
}

pub fn series_startswith(s: &Series, pattern: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern_arr| {
            arr.binary_broadcasted_compare(
                pattern_arr,
                |data: &str, pat: &str| Ok(data.starts_with(pat)),
                "startswith",
            )
            .map(IntoSeries::into_series)
        })
    })
}
