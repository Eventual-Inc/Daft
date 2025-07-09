use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_numeric;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Ceil;

#[typetag::serde]
impl ScalarUDF for Ceil {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        match input.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(input.clone()),
            DataType::Float32 => Ok(input.f32().unwrap().ceil()?.into_series()),
            DataType::Float64 => Ok(input.f64().unwrap().ceil()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "ceil not implemented for {}",
                dt
            ))),
        }
    }

    fn name(&self) -> &'static str {
        "ceil"
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
        "Rounds a number up to the nearest integer."
    }
}

#[must_use]
pub fn ceil(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Ceil {}, vec![input]).into()
}
