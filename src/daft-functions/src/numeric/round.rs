use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Float32Array, Float64Array, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use num_traits::Pow;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Round;

#[derive(FunctionArgs)]
struct RoundArgs<T> {
    input: T,
    #[arg(optional)]
    decimals: Option<u32>,
}

#[typetag::serde]
impl ScalarUDF for Round {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let RoundArgs { input, decimals } = inputs.try_into()?;

        let decimals = decimals.unwrap_or(0);

        series_round(&input, decimals as i32)
    }

    fn name(&self) -> &'static str {
        "round"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let RoundArgs { input, .. } = inputs.try_into()?;

        let field = input.to_field(schema)?;

        let dtype = field.dtype.to_floating_representation()?;
        Ok(Field::new(field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Rounds a number to a specified number of decimal places."
    }
}

#[must_use]
pub fn round(input: ExprRef, decimal: Option<ExprRef>) -> ExprRef {
    let mut inputs = vec![input];
    if let Some(decimal) = decimal {
        inputs.push(decimal);
    }
    ScalarFunction::new(Round {}, inputs).into()
}

pub fn series_round(s: &Series, decimal: i32) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => s.clone().cast(&s.to_floating_data_type()?),
        DataType::Float32 => Ok(f32_round(s.f32().unwrap(), decimal)?.into_series()),
        DataType::Float64 => Ok(f64_round(s.f64().unwrap(), decimal)?.into_series()),
        dt => Err(DaftError::TypeError(format!(
            "round not implemented for {}",
            dt
        ))),
    }
}

fn f32_round(arr: &Float32Array, precision: i32) -> DaftResult<Float32Array> {
    if precision == 0 {
        arr.apply(|v| v.round())
    } else {
        let multiplier: f64 = 10.0.pow(precision);
        arr.apply(|v| ((v as f64 * multiplier).round() / multiplier) as f32)
    }
}

fn f64_round(arr: &Float64Array, precision: i32) -> DaftResult<Float64Array> {
    if precision == 0 {
        arr.apply(|v| v.round())
    } else {
        let multiplier: f64 = 10.0.pow(precision);
        arr.apply(|v| ((v * multiplier).round() / multiplier))
    }
}
