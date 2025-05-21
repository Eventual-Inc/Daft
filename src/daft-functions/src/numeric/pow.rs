use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Pow;

#[typetag::serde]
impl ScalarUDF for Pow {
    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let exp = inputs.required((1, "by"))?;
        let exp: f64 = {
            ensure!(exp.len() == 1, "expected scalar value");
            let s = exp.cast(&DataType::Float64)?;

            s.f64().unwrap().get(0).unwrap()
        };

        pow_impl(input, exp)
    }

    fn name(&self) -> &'static str {
        "pow"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to compute exp to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Calculate the nth-power of a number (number^n)."
    }
}

#[must_use]
pub fn pow(input: ExprRef, exp: ExprRef) -> ExprRef {
    ScalarFunction::new(Pow {}, vec![input, exp]).into()
}

fn pow_impl(s: &Series, exp: f64) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Float32 => Ok(s.f32().unwrap().pow(exp)?.into_series()),
        DataType::Float64 => Ok(s.f64().unwrap().pow(exp)?.into_series()),
        dt if dt.is_integer() => pow_impl(&s.cast(&DataType::Float64)?, exp),
        dt => Err(DaftError::TypeError(format!(
            "pow not implemented for {}",
            dt
        ))),
    }
}
