use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::{functions::FunctionExpr, ExprRef};
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, ImageExpr};

pub struct EncodeEvaluator {}

impl FunctionEvaluator for EncodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::Image(..) | DataType::FixedShapeImage(..) => {
                        Ok(Field::new(field.name, DataType::Binary))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "ImageEncode can only encode ImageArrays and FixedShapeImageArrays, got {}",
                        field
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let image_format = match expr {
            FunctionExpr::Image(ImageExpr::Encode { image_format }) => image_format,
            _ => panic!("Expected ImageEncode Expr, got {expr}"),
        };
        match inputs {
            [input] => input.image_encode(*image_format),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
