use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Pow;

#[derive(FunctionArgs)]
struct PowArgs<T> {
    input: T,
    exp: f64,
}

#[typetag::serde]
impl ScalarUDF for Pow {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let PowArgs { input, exp } = inputs.try_into()?;

        input.pow(exp)
    }

    fn name(&self) -> &'static str {
        "pow"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required(0)?;
        let field = input.to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to compute pow to be numeric, got {}",
                    field.dtype
                )));
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
    ScalarFn::builtin(Pow {}, vec![input, exp]).into()
}
