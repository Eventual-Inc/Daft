use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

/// ex:
/// ```text
/// image_encode(input, image_format='png')
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageEncode;

#[derive(FunctionArgs)]
struct ImageEncodeArgs<T> {
    input: T,
    image_format: ImageFormat,
}

#[typetag::serde]
impl ScalarUDF for ImageEncode {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let ImageEncodeArgs {
            input,
            image_format,
        } = inputs.try_into()?;

        crate::series::encode(&input, image_format)
    }

    fn name(&self) -> &'static str {
        "image_encode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageEncodeArgs { input, .. } = inputs.try_into()?;

        let field = input.to_field(schema)?;
        match field.dtype {
            DataType::Image(..) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::Binary))
            }
            _ => Err(DaftError::TypeError(format!(
                "ImageEncode can only encode ImageArrays and FixedShapeImageArrays, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Encodes an image into the specified image file format, returning a binary column of encoded bytes."
    }
}
