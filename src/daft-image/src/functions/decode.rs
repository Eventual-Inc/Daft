use common_error::{ensure, DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

/// ```text
/// image_decode(input)
/// image_decode(input, mode='RGB')
/// image_decode(input, mode='RGB', on_error='raise')
/// image_decode(input, on_error='null')
/// image_decode(input, on_error='null', mode='RGB')
/// image_decode(input, mode='RGB', on_error='null')
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageDecode;

#[typetag::serde]
impl ScalarUDF for ImageDecode {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let image_mode = inputs.optional("mode")?;
        let on_error = inputs.optional(("on_error", "raise_on_error"))?;
        let image_mode = match image_mode {
            Some(s) => {
                ensure!(s.len() == 1, "image_mode must be a scalar value");

                if s.data_type().is_null() {
                    None
                } else {
                    let value = s.utf8()?.get(0).unwrap();
                    Some(value.parse()?)
                }
            }
            None => None,
        };

        let on_error = on_error
            .map(|s| {
                ensure!(s.len() == 1, "OnError must be a scalar value");
                if s.data_type().is_null() {
                    return Ok("raise");
                }
                Ok(s.utf8()?.get(0).unwrap())
            })
            .transpose()?
            .unwrap_or("raise");

        let on_error = match on_error.to_lowercase().as_str() {
            "raise" => true,
            "null" => false,
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Invalid on_error value: {}",
                    on_error
                )))
            }
        };

        crate::series::decode(input, on_error, image_mode)
    }

    fn name(&self) -> &'static str {
        "image_decode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            !inputs.is_empty() && inputs.len() <= 3,
            "ImageDecode requires between 1 and 3 inputs"
        );

        let field = inputs.required((0, "input"))?.to_field(schema)?;

        if !matches!(field.dtype, DataType::Binary) {
            return Err(DaftError::TypeError(format!(
                "ImageDecode can only decode BinaryArrays, got {field}"
            )));
        }

        let image_mode: Option<ImageMode> = inputs
            .optional("mode")?
            .and_then(|e| e.as_literal())
            .and_then(|lit| lit.as_str())
            .map(|s| s.parse())
            .transpose()?;

        if let Some(on_error) = inputs.optional("on_error")? {
            let f = on_error.to_field(schema)?;
            ensure!(f.dtype == DataType::Utf8, "on_error must be a string");
        };

        Ok(Field::new(field.name, DataType::Image(image_mode)))
    }

    fn docstring(&self) -> &'static str {
        "Decodes an image from binary data. Optionally, you can specify the image mode and error handling behavior."
    }
}
