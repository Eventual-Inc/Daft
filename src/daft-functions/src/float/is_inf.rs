use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
    with_match_float_and_null_daft_types,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IsInf;

#[typetag::serde]
impl ScalarUDF for IsInf {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        use daft_core::{array::ops::DaftIsInf, series::IntoSeries};
        let UnaryArg { input: data } = inputs.try_into()?;

        with_match_float_and_null_daft_types!(data.data_type(), |$T| {
            Ok(DaftIsInf::is_inf(data.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
        })
    }

    fn name(&self) -> &'static str {
        "is_inf"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input: data } = inputs.try_into()?;
        let data = data.to_field(schema)?;
        match &data.dtype {
            DataType::Null | DataType::Float32 | DataType::Float64 => {
                Ok(Field::new(data.name, DataType::Boolean))
            }
            _ => Err(DaftError::TypeError(format!(
                "Expects input to is_inf to be float, but received {data}",
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Checks if the input expression is infinite (positive or negative infinity)."
    }
}

#[must_use]
pub fn is_inf(input: ExprRef) -> ExprRef {
    ScalarFunction::new(IsInf {}, vec![input]).into()
}
