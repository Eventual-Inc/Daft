use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageCrop;

#[typetag::serde]
impl ScalarUDF for ImageCrop {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "image_crop"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, bbox] => {
                let input_field = input.to_field(schema)?;
                let bbox_field = bbox.to_field(schema)?;

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
                        return Err(DaftError::TypeError(
                            format!(
                            "bbox list field must be List with numeric child type or FixedSizeList with size 4, got {dtype}"
                            )
                        ));
                    }
                }

                // Validate the input field type
                match &input_field.dtype {
                    DataType::Image(_) => Ok(input_field.clone()),
                    DataType::FixedShapeImage(mode, ..) => {
                        Ok(Field::new(input_field.name, DataType::Image(Some(*mode))))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Image crop can only crop ImageArrays and FixedShapeImage, got {input_field}"
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input, bbox] => crate::series::crop(input, bbox),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Crops an image to a specified bounding box. The bounding box is specified as [x, y, width, height]."
    }
}
