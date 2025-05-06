use common_error::{ensure, DaftResult};
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
pub struct Lower;

#[typetag::serde]
impl ScalarUDF for Lower {
    fn name(&self) -> &'static str {
        "lower"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "lower requires exactly 1 input");

        let input = inputs.required((0, "input"))?;
        if input.data_type().is_null() {
            return Ok(Series::full_null(
                input.name(),
                &DataType::Null,
                input.len(),
            ));
        }
        series_lower(input)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;

        if input.dtype.is_null() {
            Ok(Field::new(input.name, DataType::Null))
        } else {
            ensure!(
                input.dtype.is_string(),
                TypeError: "Expects input to lower to be utf8, but received {}", input.dtype
            );

            Ok(Field::new(input.name, DataType::UInt64))
        }
    }

    fn docstring(&self) -> &'static str {
        "Converts a string to lowercase."
    }
}

#[must_use]
pub fn lower(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Lower, vec![input]).into()
}

pub fn series_lower(s: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        Ok(arr
            .unary_broadcasted_op(|val| val.to_lowercase().into())?
            .into_series())
    })
}
