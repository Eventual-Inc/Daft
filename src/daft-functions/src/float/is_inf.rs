use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
    with_match_float_and_null_daft_types,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IsInf {}

#[typetag::serde]
impl ScalarUDF for IsInf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "is_inf"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Null | DataType::Float32 | DataType::Float64 => {
                        Ok(Field::new(data_field.name, DataType::Boolean))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to is_inf to be float, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        use daft_core::{array::ops::DaftIsInf, series::IntoSeries};
        match inputs {
            [data] => {
                with_match_float_and_null_daft_types!(data.data_type(), |$T| {
                    Ok(DaftIsInf::is_inf(data.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn is_inf(input: ExprRef) -> ExprRef {
    ScalarFunction::new(IsInf {}, vec![input]).into()
}
