use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use daft_schema::image_property::ImageProperty;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageAttribute;

#[derive(FunctionArgs)]
struct ImageAttributeArgs<T> {
    input: T,
    attr: ImageProperty,
}

#[typetag::serde]
impl ScalarUDF for ImageAttribute {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let ImageAttributeArgs { input, attr } = inputs.try_into()?;
        crate::series::attribute(&input, attr)
    }

    fn name(&self) -> &'static str {
        "image_attribute"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageAttributeArgs { input, .. } = inputs.try_into()?;

        let input_field = input.to_field(schema)?;
        match input_field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(input_field.name, DataType::UInt32))
            }
            _ => Err(DaftError::TypeError(format!(
                "Image attribute can only be retrieved from ImageArrays, got {}",
                input_field.dtype
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Extracts metadata attributes from image series (height/width/channels/mode)"
    }
}
