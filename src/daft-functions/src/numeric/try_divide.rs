use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::DaftCompare,
    datatypes::{InferDataType, Int64Array},
    prelude::{Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryDivide;

#[derive(FunctionArgs)]
struct TryDivideArgs<T> {
    dividend: T,
    divisor: T,
}

#[typetag::serde]
impl ScalarUDF for TryDivide {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let TryDivideArgs { dividend, divisor } = inputs.try_into()?;
        try_divide_impl(dividend, divisor)
    }

    fn name(&self) -> &'static str {
        "try_divide"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let TryDivideArgs { dividend, divisor } = inputs.try_into()?;
        let dividend_field = dividend.to_field(schema)?;
        let divisor_field = divisor.to_field(schema)?;
        if !dividend_field.dtype.is_numeric() || !divisor_field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected inputs to try_divide to be numeric, got {} and {}",
                dividend_field.dtype, divisor_field.dtype
            )));
        }
        let dtype = (InferDataType::from(&dividend_field.dtype)
            / InferDataType::from(&divisor_field.dtype))?;
        Ok(Field::new(dividend_field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Returns dividend divided by divisor, or NULL if the divisor is 0."
    }
}

fn try_divide_impl(dividend: Series, divisor: Series) -> DaftResult<Series> {
    let quotient = (&dividend / &divisor)?;
    // Null out rows where the divisor is zero, matching Spark's DivModLike
    // (arithmetic.scala): try_divide is NULL on a zero divisor for every
    // numeric type, including floats that would otherwise give inf/NaN.
    let divisor = if divisor.len() == 1 && quotient.len() != 1 {
        divisor.broadcast(quotient.len())?
    } else {
        divisor
    };
    let zero = Int64Array::from_values("zero", std::iter::once(0))
        .into_series()
        .cast(divisor.data_type())?
        .broadcast(divisor.len())?;
    // Daft float equality is bitwise, so `-0.0 == 0` is false. Adding zero
    // first normalizes `-0.0` to `+0.0` (IEEE 754) and is a no-op otherwise.
    let normalized = (&divisor + &zero)?;
    let is_zero = normalized.equal(&zero)?.into_series();
    let nulls = Series::full_null(quotient.name(), quotient.data_type(), quotient.len());
    nulls.if_else(&quotient, &is_zero)
}

#[must_use]
pub fn try_divide(dividend: ExprRef, divisor: ExprRef) -> ExprRef {
    ScalarFn::builtin(TryDivide {}, vec![dividend, divisor]).into()
}
