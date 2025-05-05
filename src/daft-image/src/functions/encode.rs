use common_error::{ensure, DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

/// ex:
/// ```text
/// image_encode(input, image_format='png')
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageEncode;

#[typetag::serde]
impl ScalarUDF for ImageEncode {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, "image_encode requires 2 arguments");

        let input = inputs.required((0, "input"))?;
        let image_format = inputs.required((1, "image_format"))?;
        let image_format = {
            ensure!(image_format.len() == 1, "image format must be scalar value");

            let image_format = image_format
                .utf8()
                .expect("already checked in `to_field`")
                .get(0)
                .unwrap();

            image_format.parse()?
        };

        crate::series::encode(input, image_format)
    }

    fn name(&self) -> &'static str {
        "image_encode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, "image_encode requires 2 arguments");

        let input = inputs.required((0, "input"))?;
        let image_format = inputs.required((1, "image_format"))?;
        let field = input.to_field(schema)?;
        let image_format = image_format.to_field(schema)?;
        if image_format.dtype != DataType::Utf8 {
            Err(DaftError::TypeError(format!(
                "ImageEncode requires image_format to be a string, got {image_format}"
            )))
        } else {
            match field.dtype {
                DataType::Image(..) | DataType::FixedShapeImage(..) => {
                    Ok(Field::new(field.name, DataType::Binary))
                }
                _ => Err(DaftError::TypeError(format!(
                    "ImageEncode can only encode ImageArrays and FixedShapeImageArrays, got {field}"
                ))),
            }
        }
    }

    fn docstring(&self) -> &'static str {
        "Encodes an image into the specified image file format, returning a binary column of encoded bytes."
    }
}
