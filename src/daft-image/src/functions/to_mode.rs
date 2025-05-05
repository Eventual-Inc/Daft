use common_error::{ensure, DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageToMode;

#[typetag::serde]
impl ScalarUDF for ImageToMode {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let image_mode = inputs.required("mode").and_then(|s| {
            ensure!(s.len() == 1, "ImageMode must be a scalar value");
            let value = s.utf8()?.get(0).unwrap();
            value.parse()
        })?;

        crate::series::to_mode(input, image_mode)
    }

    fn name(&self) -> &'static str {
        "to_mode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, "ImageToMode requires exactly 2 inputs");

        let field = inputs.required((0, "input"))?.to_field(schema)?;
        let image_mode = inputs.required("mode")?;
        let image_mode = image_mode
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| DaftError::ValueError("mode must be a string".to_string()))
            .and_then(|mode| mode.parse())?;

        let output_dtype = match field.dtype {
            DataType::Image(_) => DataType::Image(Some(image_mode)),
            DataType::FixedShapeImage(_, h, w) => DataType::FixedShapeImage(image_mode, h, w),
            _ => {
                return Err(DaftError::TypeError(format!(
                    "ToMode can only operate on ImageArrays and FixedShapeImageArrays, got {field}"
                )))
            }
        };

        Ok(Field::new(field.name, output_dtype))
    }
    fn docstring(&self) -> &'static str {
        "Converts an image to the specified mode (e.g. RGB, RGBA, Grayscale)."
    }
}
