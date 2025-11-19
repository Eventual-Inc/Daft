use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use super::to_field_floating;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Sqrt;

#[typetag::serde]
impl ScalarUDF for Sqrt {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        let casted_dtype = input.to_floating_data_type()?;
        let casted_self = input
            .cast(&casted_dtype)
            .expect("Casting numeric types to their floating point analogues should not fail");
        match casted_dtype {
            DataType::Float32 => Ok(casted_self.f32().unwrap().sqrt()?.into_series()),
            DataType::Float64 => Ok(casted_self.f64().unwrap().sqrt()?.into_series()),
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &'static str {
        "sqrt"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        to_field_floating(&input, schema)
    }

    fn docstring(&self) -> &'static str {
        "Returns the square root of a number."
    }
}

#[must_use]
pub fn sqrt(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Sqrt {}, vec![input]).into()
}
