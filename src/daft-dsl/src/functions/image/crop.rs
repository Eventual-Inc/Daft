use crate::Expr;
use common_error::DaftError;
use daft_core::datatypes::DataType;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use common_error::DaftResult;

use super::super::FunctionEvaluator;

pub struct CropEvaluator {}

impl FunctionEvaluator for CropEvaluator {
    fn fn_name(&self) -> &'static str {
        "crop"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _expr: &Expr) -> DaftResult<Field> {
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
                    _ => {
                        return Err(DaftError::TypeError(
                            "bbox list field must be List with numeric child type".to_string(),
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
                        "Image crop can only crop ImageArrays and FixedShapeImage, got {}",
                        input_field
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input, bbox] => input.image_crop(bbox),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
