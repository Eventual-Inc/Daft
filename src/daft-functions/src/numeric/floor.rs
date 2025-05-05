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
pub struct Floor;

#[typetag::serde]
impl ScalarUDF for Floor {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "floor expects 1 argument");
        let s = inputs.required((0, "input"))?;

        s.floor()
    }

    fn name(&self) -> &'static str {
        "floor"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Rounds a number down to the nearest integer."
    }
}

#[must_use]
pub fn floor(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Floor {}, vec![input]).into()
}
