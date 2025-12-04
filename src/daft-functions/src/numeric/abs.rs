use common_error::DaftResult;
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use super::to_field_numeric;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Abs;

#[typetag::serde]
impl ScalarUDF for Abs {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input.abs()
    }

    fn name(&self) -> &'static str {
        "abs"
    }

    fn get_return_field(
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
    ScalarFn::builtin(Abs {}, vec![input]).into()
}
