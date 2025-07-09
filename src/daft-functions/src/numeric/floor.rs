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
pub struct Floor;

#[typetag::serde]
impl ScalarUDF for Floor {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input.floor()
    }

    fn name(&self) -> &'static str {
        "floor"
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
        "Rounds a number down to the nearest integer."
    }
}

#[must_use]
pub fn floor(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Floor {}, vec![input]).into()
}
