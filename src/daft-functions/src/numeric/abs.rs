use common_error::DaftResult;
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_numeric;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Abs;

#[typetag::serde]
impl ScalarUDF for Abs {
    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input.abs()
    }

    fn name(&self) -> &'static str {
        "abs"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        to_field_numeric(self, &input, schema)
    }

    fn docstring(&self) -> &'static str {
        "Gets the absolute value of a number."
    }
}

#[must_use]
pub fn abs(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Abs {}, vec![input]).into()
}
