use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{BinaryArray, DataType, Field, FixedSizeBinaryArray},
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
    fn name(&self) -> &'static str {
        "length"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let data = &inputs[0];
        match data.to_field(schema) {
            Ok(data_field) => match &data_field.dtype {
                DataType::Binary | DataType::FixedSizeBinary(_) => {
                    Ok(Field::new(data_field.name, DataType::UInt64))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Expects input to length to be binary, but received {data_field}",
                ))),
            },
            Err(e) => Err(e),
        }
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }
    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs[0].data_type() {
            DataType::Binary => {
                let binary_array = inputs[0].downcast::<BinaryArray>()?;
                let result = binary_array.length()?;
                Ok(result.into_series())
            }
            DataType::FixedSizeBinary(_size) => {
                let binary_array = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let result = binary_array.length()?;
                Ok(result.into_series())
            }
            _ => unreachable!("Type checking is done in to_field"),
        }
    }
}

#[must_use]
pub fn binary_length(input: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryLength {}, vec![input]).into()
}
