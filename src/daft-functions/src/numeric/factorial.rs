use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::Int64Array,
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
        if !field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to factorial to be integer, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Int64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the factorial of a non-negative integer."
    }
}

fn compute_factorial(n: i64) -> Option<i64> {
    if !(0..=20).contains(&n) {
        return None;
    }
    let mut result: i64 = 1;
    for i in 2..=n {
        result *= i;
    }
    Some(result)
}

fn factorial_impl(s: Series) -> DaftResult<Series> {
    let casted = s.cast(&DataType::Int64)?;
    let i64_arr = casted.i64().unwrap();
    let field = Field::new(i64_arr.name(), DataType::Int64);
    let result =
        Int64Array::from_iter(field, i64_arr.iter().map(|v| v.and_then(compute_factorial)));
    Ok(result.into_series())
}

#[must_use]
pub fn factorial(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Factorial {}, vec![input]).into()
}
