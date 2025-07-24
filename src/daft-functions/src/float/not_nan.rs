use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::DaftNotNan,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
    with_match_float_and_null_daft_types,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NotNan;

#[typetag::serde]
impl ScalarUDF for NotNan {
    fn name(&self) -> &'static str {
        "not_nan"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input: data } = inputs.try_into()?;

        with_match_float_and_null_daft_types!(data.data_type(), |$T| {
            Ok(DaftNotNan::not_nan(data.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
        })
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
                "Expects input to not_nan to be float, but received {data_field}",
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Checks if the input expression is not NaN (Not a Number)."
    }
}

#[must_use]
pub fn not_nan(input: ExprRef) -> ExprRef {
    ScalarFunction::new(NotNan {}, vec![input]).into()
}
