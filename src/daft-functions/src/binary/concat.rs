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
pub struct BinaryConcat {}

#[typetag::serde]
impl ScalarUDF for BinaryConcat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "concat"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [left, right] => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;
                match (&left_field.dtype, &right_field.dtype) {
                    (DataType::Binary, DataType::Binary) => Ok(Field::new(left_field.name, DataType::Binary)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to concat to be binary, but received {left_field} and {right_field}",
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let left_array = inputs[0].downcast::<BinaryArray>()?;
        let right_array = inputs[1].downcast::<BinaryArray>()?;
        let result = left_array.binary_concat(right_array)?;
        Ok(result.into_series())
    }
}

pub fn binary_concat(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryConcat {}, vec![left, right]).into()
}
