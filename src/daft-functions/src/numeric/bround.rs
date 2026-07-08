use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::Pow;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Bround;

#[derive(FunctionArgs)]
struct BroundArgs<T> {
    input: T,
    #[arg(optional)]
    decimals: Option<u32>,
}

#[typetag::serde]
impl ScalarUDF for Bround {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let BroundArgs { input, decimals } = inputs.try_into()?;
        let decimals = decimals.unwrap_or(0);
        bround_impl(&input, decimals as i32)
    }

    fn name(&self) -> &'static str {
        "bround"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let BroundArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let dtype = field.dtype.to_floating_representation()?;
        Ok(Field::new(field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Rounds a number to a specified number of decimal places using banker's rounding (round half to even). For example, bround(2.5) == 2.0 and bround(3.5) == 4.0."
    }
}

#[must_use]
pub fn bround(input: ExprRef, decimal: Option<ExprRef>) -> ExprRef {
    let mut inputs = vec![input];
    if let Some(decimal) = decimal {
        inputs.push(decimal);
    }
    ScalarFn::builtin(Bround {}, inputs).into()
}

fn bround_impl(s: &Series, decimal: i32) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => s.clone().cast(&s.to_floating_data_type()?),
        DataType::Float16 => {
            let s32 = s.cast(&DataType::Float32)?;
            let ca = s32.f32().unwrap();
            let result = ca.apply(|v| banker_round_f32(v, decimal))?;
            result.into_series().cast(&DataType::Float16)
        }
        DataType::Float32 => {
            let ca = s.f32().unwrap();
            Ok(ca.apply(|v| banker_round_f32(v, decimal))?.into_series())
        }
        DataType::Float64 => {
            let ca = s.f64().unwrap();
            Ok(ca.apply(|v| banker_round_f64(v, decimal))?.into_series())
        }
        dt => Err(DaftError::TypeError(format!(
            "bround not implemented for {}",
            dt
        ))),
    }
}

fn banker_round_f32(v: f32, decimal: i32) -> f32 {
    if decimal == 0 {
        v.round_ties_even()
    } else {
        let multiplier: f64 = 10.0.pow(decimal);
        ((v as f64 * multiplier).round_ties_even() / multiplier) as f32
    }
}

fn banker_round_f64(v: f64, decimal: i32) -> f64 {
    if decimal == 0 {
        v.round_ties_even()
    } else {
        let multiplier: f64 = 10.0.pow(decimal);
        (v * multiplier).round_ties_even() / multiplier
    }
}
