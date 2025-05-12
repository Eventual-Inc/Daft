use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::to_field_single_numeric;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Ceil;

#[typetag::serde]
impl ScalarUDF for Ceil {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "ceil expects 1 argument");
        let s = inputs.required((0, "input"))?;
        match s.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(s.clone()),
            DataType::Float32 => Ok(s.f32().unwrap().ceil()?.into_series()),
            DataType::Float64 => Ok(s.f64().unwrap().ceil()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "ceil not implemented for {}",
                dt
            ))),
        }
    }

    fn name(&self) -> &'static str {
        "ceil"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Rounds a number up to the nearest integer."
    }
}

#[must_use]
pub fn ceil(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Ceil {}, vec![input]).into()
}
