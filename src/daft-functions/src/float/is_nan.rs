use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::ops::DaftIsNan,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
    with_match_float_and_null_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IsNan;

#[typetag::serde]
impl ScalarUDF for IsNan {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, ComputeError: "Expected 1 input, got {}", inputs.len());

        let data = inputs.required(("input", 0))?;

        with_match_float_and_null_daft_types!(data.data_type(), |$T| {
            Ok(DaftIsNan::is_nan(data.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
        })
    }

    fn name(&self) -> &'static str {
        "is_nan"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let data_field = input.to_field(schema)?;
        match &data_field.dtype {
            // DataType::Float16 |
            DataType::Null | DataType::Float32 | DataType::Float64 => {
                Ok(Field::new(data_field.name, DataType::Boolean))
            }
            _ => Err(DaftError::TypeError(format!(
                "Expects input to is_nan to be float, but received {data_field}",
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Checks if the input expression is NaN (Not a Number)."
    }
}

#[must_use]
pub fn is_nan(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(IsNan {}, vec![input]).into()
}
