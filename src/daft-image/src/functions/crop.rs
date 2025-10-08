use common_error::{DaftError, DaftResult, ensure};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageCrop;

#[typetag::serde]
impl ScalarUDF for ImageCrop {
    fn name(&self) -> &'static str {
        "image_crop"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, "expected 2 inputs");

        let input = inputs.required((0, "input"))?;
        let bbox = inputs.required((1, "bbox"))?;
        crate::series::crop(input, bbox)
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, "image_crop requires 2 arguments");

        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let bbox_field = inputs.required((1, "bbox"))?.to_field(schema)?;
        // Validate the bbox field type
        match &bbox_field.dtype {
            DataType::FixedSizeList(_, size) if *size != 4 => {
                return Err(DaftError::TypeError(
                    "bbox FixedSizeList field must have size 4 for cropping".to_string(),
                ));
            }
            DataType::FixedSizeList(child_dtype, _) | DataType::List(child_dtype)
                if !child_dtype.is_numeric() =>
            {
                return Err(DaftError::TypeError(
                    "bbox list field must have numeric child type".to_string(),
                ));
            }
            DataType::FixedSizeList(..) | DataType::List(..) => (),
            dtype => {
                return Err(DaftError::TypeError(format!(
                    "bbox list field must be List with numeric child type or FixedSizeList with size 4, got {dtype}"
                )));
            }
        }

        match &input.dtype {
            DataType::Image(_) => Ok(input.clone()),
            DataType::FixedShapeImage(mode, ..) => {
                Ok(Field::new(input.name, DataType::Image(Some(*mode))))
            }
            _ => Err(DaftError::TypeError(format!(
                "Image crop can only crop ImageArrays and FixedShapeImage, got {input}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Crops an image to a specified bounding box. The bounding box is specified as [x, y, width, height]."
    }
}
