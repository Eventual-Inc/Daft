use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{BinaryArray, DataType, Field},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryLength {}

#[typetag::serde]
impl ScalarUDF for BinaryLength {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "length"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Binary => Ok(Field::new(data_field.name, DataType::UInt64)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to length to be binary, but received {data_field}",
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
        match inputs {
            [data] => {
                let binary_array = data.downcast::<BinaryArray>()?;
                let result = binary_array.length()?;
                Ok(result.into_series())
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn binary_length(input: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryLength {}, vec![input]).into()
}
