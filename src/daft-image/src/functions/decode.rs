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

#[derive(FunctionArgs)]
struct ImageDecodeArgs<T> {
    input: T,
    #[arg(optional)]
    mode: Option<ImageMode>,
    #[arg(optional)]
    on_error: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for ImageDecode {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let ImageDecodeArgs {
            input,
            mode,
            on_error,
        } = inputs.try_into()?;

        let on_error = on_error
            .map(|s| {
                ensure!(s.len() == 1, "OnError must be a scalar value");
                if s.data_type().is_null() {
                    return Ok("raise".to_string());
                }
                Ok(s.utf8()?.get(0).unwrap().to_string())
            })
            .transpose()?
            .unwrap_or_else(|| "raise".to_string());

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

        crate::series::decode(&input, on_error, mode)
    }

    fn name(&self) -> &'static str {
        "image_decode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageDecodeArgs {
            input,
            mode,
            on_error,
        } = inputs.try_into()?;

        let field = input.to_field(schema)?;

        if !matches!(field.dtype, DataType::Binary) {
            return Err(DaftError::TypeError(format!(
                "ImageDecode can only decode BinaryArrays, got {field}"
            )));
        }

        if let Some(on_error) = on_error {
            let f = on_error.to_field(schema)?;
            ensure!(f.dtype == DataType::Utf8, "on_error must be a string");
        };

        Ok(Field::new(field.name, DataType::Image(mode)))
    }

    fn docstring(&self) -> &'static str {
        "Decodes an image from binary data. Optionally, you can specify the image mode and error handling behavior."
    }
}
