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

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field, Utf8ArrayUtils};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Reverse;

#[typetag::serde]
impl ScalarUDF for Reverse {
    fn name(&self) -> &'static str {
        "reverse"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                Ok(arr
                    .unary_broadcasted_op(|val| val.chars().rev().collect::<String>().into())?
                    .into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "Reverse a UTF-8 string."
    }
}

#[must_use]
pub fn reverse(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Reverse {}, vec![input]).into()
}
