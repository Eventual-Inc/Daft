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
pub struct Contains;

#[typetag::serde]
impl ScalarUDF for Contains {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", contains_impl)
    }

    fn get_return_field(
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

fn contains_impl(s: &Series, pattern: &Series) -> DaftResult<Series> {
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
