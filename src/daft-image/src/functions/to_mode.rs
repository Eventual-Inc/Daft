use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageToMode;

#[derive(FunctionArgs)]
struct ImageToModeArgs<T> {
    input: T,
    mode: ImageMode,
}

#[typetag::serde]
impl ScalarUDF for ImageToMode {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let ImageToModeArgs { input, mode } = inputs.try_into()?;

        crate::series::to_mode(&input, mode)
    }

    fn name(&self) -> &'static str {
        "to_mode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageToModeArgs { input, mode } = inputs.try_into()?;

        let field = input.to_field(schema)?;
        let output_dtype = match field.dtype {
            DataType::Image(_) => DataType::Image(Some(mode)),
            DataType::FixedShapeImage(_, h, w) => DataType::FixedShapeImage(mode, h, w),
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
