use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, unary_utf8_evaluate, unary_utf8_to_field};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Upper;

#[typetag::serde]
impl ScalarUDF for Upper {
    fn name(&self) -> &'static str {
        "upper"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                Ok(arr
                    .unary_broadcasted_op(|val| val.to_uppercase().into())?
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
        "Converts a string to uppercase."
    }
}

#[must_use]
pub fn upper(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Upper {}, vec![input]).into()
}
