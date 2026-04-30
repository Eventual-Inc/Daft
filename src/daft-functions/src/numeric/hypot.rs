use daft_common::error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Hypot;

#[derive(FunctionArgs)]
struct HypotArgs<T> {
    a: T,
    b: T,
}

#[typetag::serde]
impl ScalarUDF for Hypot {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let HypotArgs { a, b } = inputs.try_into()?;
        hypot_impl(a, b)
    }

    fn name(&self) -> &'static str {
        "hypot"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let HypotArgs { a, b } = inputs.try_into()?;
        let a_field = a.to_field(schema)?;
        let b_field = b.to_field(schema)?;
        let dtype = match (a_field.dtype, b_field.dtype) {
            (DataType::Float32, DataType::Float32) => DataType::Float32,
            (dt1, dt2) if dt1.is_numeric() && dt2.is_numeric() => DataType::Float64,
            (dt1, dt2) => {
                return Err(DaftError::TypeError(format!(
                    "Expected inputs to hypot to be numeric, got {dt1} and {dt2}"
                )));
            }
        };
        Ok(Field::new(a_field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Returns sqrt(a^2 + b^2), the Euclidean norm."
    }
}

fn hypot_impl(a: Series, b: Series) -> DaftResult<Series> {
    match (a.data_type(), b.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            let a_ca = a.f32().unwrap();
            let b_ca = b.f32().unwrap();
            Ok(a_ca.binary_apply(b_ca, |x, y| x.hypot(y))?.into_series())
        }
        (DataType::Float64, DataType::Float64) => {
            let a_ca = a.f64().unwrap();
            let b_ca = b.f64().unwrap();
            Ok(a_ca.binary_apply(b_ca, |x, y| x.hypot(y))?.into_series())
        }
        (DataType::Float64, rhs_dt) if rhs_dt.is_numeric() => {
            let b_s = b.cast(&DataType::Float64)?;
            hypot_impl(a, b_s)
        }
        (lhs_dt, DataType::Float64) if lhs_dt.is_numeric() => {
            let a_s = a.cast(&DataType::Float64)?;
            hypot_impl(a_s, b)
        }
        (lhs_dt, rhs_dt) if lhs_dt.is_numeric() && rhs_dt.is_numeric() => {
            let a_s = a.cast(&DataType::Float64)?;
            let b_s = b.cast(&DataType::Float64)?;
            hypot_impl(a_s, b_s)
        }
        (lhs_dt, rhs_dt) => Err(DaftError::TypeError(format!(
            "Expected inputs to hypot to be numeric, got {} and {}",
            lhs_dt, rhs_dt
        ))),
    }
}

#[must_use]
pub fn hypot(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(Hypot {}, vec![a, b]).into()
}
