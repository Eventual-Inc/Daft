use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_single_numeric;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Abs;

#[typetag::serde]
impl ScalarUDF for Abs {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "abs expects 1 argument");
        let s = inputs.required((0, "input"))?;
        s.abs()
    }

    fn name(&self) -> &'static str {
        "abs"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Gets the absolute value of a number."
    }
}

#[must_use]
pub fn abs(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Abs {}, vec![input]).into()
}
