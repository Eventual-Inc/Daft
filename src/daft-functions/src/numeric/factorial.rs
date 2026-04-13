use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Factorial;

#[typetag::serde]
impl ScalarUDF for Factorial {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        factorial_impl(input)
    }

    fn name(&self) -> &'static str {
        "factorial"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to factorial to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the factorial of a non-negative integer."
    }
}

fn compute_factorial(n: f64) -> f64 {
    if n.is_nan() || n < 0.0 || n != n.floor() {
        return f64::NAN;
    }
    let n = n as u64;
    let mut result: f64 = 1.0;
    for i in 2..=n {
        result *= i as f64;
    }
    result
}

fn factorial_impl(s: Series) -> DaftResult<Series> {
    let casted = if s.data_type() == &DataType::Float64 {
        s
    } else {
        s.cast(&DataType::Float64)?
    };
    Ok(casted
        .f64()
        .unwrap()
        .apply(compute_factorial)?
        .into_series())
}

#[must_use]
pub fn factorial(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Factorial {}, vec![input]).into()
}
