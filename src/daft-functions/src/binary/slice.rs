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
pub struct BinarySlice {}

#[typetag::serde]
impl ScalarUDF for BinarySlice {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "binary_slice"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, start, length] => {
                let data = data.to_field(schema)?;
                let start = start.to_field(schema)?;
                let length = length.to_field(schema)?;

                match &data.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(_) => {
                        if start.dtype.is_integer() && (length.dtype.is_integer() || length.dtype.is_null()) {
                            Ok(Field::new(data.name, DataType::Binary))
                        } else {
                            Err(DaftError::TypeError(format!(
                                "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                                data.dtype, start.dtype, length.dtype
                            )))
                        }
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                        data.dtype, start.dtype, length.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let data = &inputs[0];
        let start = &inputs[1];
        let length = &inputs[2];
        data.binary_slice(start, length)
    }
}

#[must_use]
pub fn binary_slice(input: ExprRef, start: ExprRef, length: ExprRef) -> ExprRef {
    ScalarFunction::new(BinarySlice {}, vec![input, start, length]).into()
}
