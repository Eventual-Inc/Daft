use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageAverageHash;

#[typetag::serde]
impl ScalarUDF for ImageAverageHash {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        crate::series::average_hash(&input)
    }
    
    fn name(&self) -> &'static str {
        "image_average_hash"
    }
    
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;
        
        match field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Utf8))
            }
            _ => Err(DaftError::TypeError(format!(
                "Average hash can only be applied to ImageArrays, got {}",
                field.dtype
            ))),
        }
    }
    
    fn docstring(&self) -> &'static str {
        "Computes the average hash of an image for deduplication. Returns an 8x8 binary hash as a 64-character string."
    }
}
