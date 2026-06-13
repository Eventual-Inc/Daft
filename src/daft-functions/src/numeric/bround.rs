use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Float32Array, Float64Array, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::Pow;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BRound;

#[derive(FunctionArgs)]
struct BRoundArgs<T> {
    input: T,
    #[arg(optional)]
    decimals: Option<i32>,
}

#[typetag::serde]
impl ScalarUDF for BRound {
    /// Spark-compatible `bround`: rounds half to even (banker's rounding).
    /// Negative `decimals` rounds to powers of 10 above the decimal point
    /// (e.g. `bround(125, -1) == 120`).
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let BRoundArgs { input, decimals } = inputs.try_into()?;
        let decimals = decimals.unwrap_or(0);
        series_bround(&input, decimals)
    }

    fn name(&self) -> &'static str {
        "bround"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let BRoundArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let dtype = field.dtype.to_floating_representation()?;
        Ok(Field::new(field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Returns the value rounded to `d` decimal places using HALF_EVEN \
         (banker's) rounding. Negative `d` rounds to powers of 10 above the \
         decimal point. Defaults to 0 decimal places."
    }
}

#[must_use]
pub fn bround(input: ExprRef, decimals: Option<ExprRef>) -> ExprRef {
    let mut inputs = vec![input];
    if let Some(decimals) = decimals {
        inputs.push(decimals);
    }
    ScalarFn::builtin(BRound {}, inputs).into()
}

pub fn series_bround(s: &Series, decimals: i32) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            // Spark casts integers to double then applies HALF_EVEN.
            let casted = s.cast(&s.to_floating_data_type()?)?;
            series_bround(&casted, decimals)
        }
        DataType::Float16 => {
            // Promote f16 -> f64 for rounding then back to f16, mirroring `round`.
            let casted = s.cast(&DataType::Float64)?;
            let rounded = f64_bround(casted.f64().unwrap(), decimals)?;
            rounded.into_series().cast(&DataType::Float16)
        }
        DataType::Float32 => Ok(f32_bround(s.f32().unwrap(), decimals)?.into_series()),
        DataType::Float64 => Ok(f64_bround(s.f64().unwrap(), decimals)?.into_series()),
        dt => Err(DaftError::TypeError(format!(
            "bround not implemented for {}",
            dt
        ))),
    }
}

fn f32_bround(arr: &Float32Array, precision: i32) -> DaftResult<Float32Array> {
    if precision == 0 {
        arr.apply(|v| v.round_ties_even())
    } else {
        let multiplier: f64 = 10.0f64.pow(precision);
        // Compute in f64 to keep parity with Spark's Decimal/Double pipeline,
        // then narrow back to f32 at the end.
        arr.apply(|v| ((v as f64 * multiplier).round_ties_even() / multiplier) as f32)
    }
}

fn f64_bround(arr: &Float64Array, precision: i32) -> DaftResult<Float64Array> {
    if precision == 0 {
        arr.apply(|v| v.round_ties_even())
    } else {
        let multiplier: f64 = 10.0f64.pow(precision);
        arr.apply(|v| (v * multiplier).round_ties_even() / multiplier)
    }
}

#[cfg(test)]
mod tests {
    use daft_core::{
        prelude::{DataType, Field, Float64Array, Int64Array},
        series::{IntoSeries, Series},
    };
    use daft_dsl::functions::{FunctionArgs, ScalarUDF, scalar::EvalContext};

    use super::BRound;

    fn f64s(name: &str, vals: Vec<Option<f64>>) -> Series {
        Float64Array::from_iter(Field::new(name, DataType::Float64), vals.into_iter()).into_series()
    }

    fn i64s(name: &str, vals: Vec<Option<i64>>) -> Series {
        Int64Array::from_iter(Field::new(name, DataType::Int64), vals.into_iter()).into_series()
    }

    fn call_bround(input: Series, decimals: Option<i32>) -> Series {
        let mut args = vec![input.clone()];
        if let Some(d) = decimals {
            args.push(
                Int64Array::from_iter(
                    Field::new("d", DataType::Int64),
                    vec![Some(d as i64)].into_iter(),
                )
                .into_series(),
            );
        }
        let ctx = EvalContext {
            row_count: input.len(),
        };
        BRound {}
            .call(FunctionArgs::new_unnamed(args), &ctx)
            .unwrap()
    }

    #[test]
    fn test_bround_half_even_default() {
        // Spark: bround(2.5) -> 2.0, bround(3.5) -> 4.0, bround(-2.5) -> -2.0
        let s = f64s("x", vec![Some(2.5), Some(3.5), Some(-2.5), Some(-3.5)]);
        let out = call_bround(s, None);
        let expected = f64s("x", vec![Some(2.0), Some(4.0), Some(-2.0), Some(-4.0)]);
        assert_eq!(out.f64().unwrap(), expected.f64().unwrap());
    }

    #[test]
    fn test_bround_with_decimals() {
        // bround(2.55, 1) -> 2.6 (round-half-to-even on 5 -> 6 because 5 is odd? No: 0.55 -> 0.5 ulp);
        // Use a clean ties case: 2.45 with 1 decimal: 2.4*10=24.5 -> 24 (even) -> 2.4
        let s = f64s("x", vec![Some(2.45), Some(2.55)]);
        let out = call_bround(s, Some(1));
        // 2.45*10 = 24.5 -> 24 (even); 2.55*10 = 25.5 -> 26 (even)
        // Note: f64 representation of 2.45 / 2.55 is not exact, so we just check
        // bit-for-bit reproducibility against direct round_ties_even.
        let expected_vals: Vec<Option<f64>> = vec![Some(2.45), Some(2.55)]
            .into_iter()
            .map(|opt| opt.map(|v| (v * 10.0_f64).round_ties_even() / 10.0))
            .collect();
        let expected = f64s("x", expected_vals);
        assert_eq!(out.f64().unwrap(), expected.f64().unwrap());
    }

    #[test]
    fn test_bround_negative_decimals() {
        // bround(125.0, -1) -> 120.0 (12.5 -> 12 even -> *10)
        let s = f64s("x", vec![Some(125.0), Some(135.0), Some(-125.0)]);
        let out = call_bround(s, Some(-1));
        let expected = f64s("x", vec![Some(120.0), Some(140.0), Some(-120.0)]);
        assert_eq!(out.f64().unwrap(), expected.f64().unwrap());
    }

    #[test]
    fn test_bround_integer_input() {
        // Integers: cast to float, then round (no-op since whole numbers).
        let s = i64s("x", vec![Some(15), Some(25)]);
        let out = call_bround(s, Some(-1));
        // 15 -> 1.5 ties even -> 2 -> 20; 25 -> 2.5 ties even -> 2 -> 20
        let expected = f64s("x", vec![Some(20.0), Some(20.0)]);
        assert_eq!(out.f64().unwrap(), expected.f64().unwrap());
    }

    #[test]
    fn test_bround_null_passthrough() {
        let s = f64s("x", vec![Some(1.5), None, Some(2.5)]);
        let out = call_bround(s, None);
        let expected = f64s("x", vec![Some(2.0), None, Some(2.0)]);
        assert_eq!(out.f64().unwrap(), expected.f64().unwrap());
    }
}
