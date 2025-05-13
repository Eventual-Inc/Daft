use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_single_floating;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Sqrt;

#[typetag::serde]
impl ScalarUDF for Sqrt {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "Expected 1 argument");
        let s = inputs.required((0, "input"))?;

        let casted_dtype = s.to_floating_data_type()?;
        let casted_self = s
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

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_floating(self, inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Returns the square root of a number."
    }
}

#[must_use]
pub fn sqrt(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Sqrt {}, vec![input]).into()
}
