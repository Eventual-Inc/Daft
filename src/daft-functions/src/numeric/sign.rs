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
pub struct Sign;

#[typetag::serde]
impl ScalarUDF for Sign {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        match input.data_type() {
            DataType::UInt8 => Ok(input.u8().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt16 => Ok(input.u16().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt32 => Ok(input.u32().unwrap().sign_unsigned()?.into_series()),
            DataType::UInt64 => Ok(input.u64().unwrap().sign_unsigned()?.into_series()),
            DataType::Int8 => Ok(input.i8().unwrap().sign()?.into_series()),
            DataType::Int16 => Ok(input.i16().unwrap().sign()?.into_series()),
            DataType::Int32 => Ok(input.i32().unwrap().sign()?.into_series()),
            DataType::Int64 => Ok(input.i64().unwrap().sign()?.into_series()),
            DataType::Float32 => Ok(input.f32().unwrap().sign()?.into_series()),
            DataType::Float64 => Ok(input.f64().unwrap().sign()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "sign not implemented for {dt}"
            ))),
        }
    }

    fn name(&self) -> &'static str {
        stringify!(sign)
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
        "Returns the sign of a number (-1, 0, or 1)."
    }
}
#[must_use]
pub fn sign(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Sign, vec![input]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Negative;

#[typetag::serde]
impl ScalarUDF for Negative {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        match input.data_type() {
            DataType::UInt8 => Ok(input
                .cast(&DataType::Int8)?
                .i8()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt8)?),
            DataType::UInt16 => Ok(input
                .cast(&DataType::Int16)?
                .i16()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt16)?),
            DataType::UInt32 => Ok(input
                .cast(&DataType::Int32)?
                .i32()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt32)?),
            DataType::UInt64 => Ok(input
                .cast(&DataType::Int64)?
                .i64()
                .unwrap()
                .negative()?
                .cast(&DataType::UInt64)?),
            DataType::Int8 => Ok(input.i8().unwrap().negative()?.into_series()),
            DataType::Int16 => Ok(input.i16().unwrap().negative()?.into_series()),
            DataType::Int32 => Ok(input.i32().unwrap().negative()?.into_series()),
            DataType::Int64 => Ok(input.i64().unwrap().negative()?.into_series()),
            DataType::Float32 => Ok(input.f32().unwrap().negative()?.into_series()),
            DataType::Float64 => Ok(input.f64().unwrap().negative()?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "negate not implemented for {}",
                dt
            ))),
        }
    }

    fn name(&self) -> &'static str {
        stringify!(negative)
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
        "Returns the negative of a number."
    }
}
#[must_use]
pub fn negative(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Negative, vec![input]).into()
}
