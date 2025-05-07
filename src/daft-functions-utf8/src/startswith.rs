use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_utf8_evaluate, binary_utf8_to_field, Utf8ArrayUtils};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StartsWith;

#[typetag::serde]
impl ScalarUDF for StartsWith {
    fn name(&self) -> &'static str {
        "starts_with"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
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
        })
    }
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::Boolean,
        )
    }
    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether each string starts with the specified pattern"
    }
}

#[must_use]
pub fn startswith(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(StartsWith {}, vec![input, pattern]).into()
}
