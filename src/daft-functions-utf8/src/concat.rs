use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{functions::prelude::*, ExprRef};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field, Utf8ArrayUtils};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Concat;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    other: T,
}

#[typetag::serde]
impl ScalarUDF for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, other } = inputs.try_into()?;
        input + other
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "Capitalize a UTF-8 string."
    }
}
