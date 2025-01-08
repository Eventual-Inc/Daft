use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinarySubstr {}

#[typetag::serde]
impl ScalarUDF for BinarySubstr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "binary_substr"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, start, length] => {
                let data = data.to_field(schema)?;
                let start = start.to_field(schema)?;
                let length = length.to_field(schema)?;

                if data.dtype == DataType::Binary
                    && start.dtype.is_integer()
                    && (length.dtype.is_integer() || length.dtype.is_null())
                {
                    Ok(Field::new(data.name, DataType::Binary))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to binary_substr to be binary, integer and integer or null but received {}, {} and {}",
                        data.dtype, start.dtype, length.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, start, length] => data.binary_substr(start, length),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn binary_substr(input: ExprRef, start: ExprRef, length: ExprRef) -> ExprRef {
    ScalarFunction::new(BinarySubstr {}, vec![input, start, length]).into()
}
