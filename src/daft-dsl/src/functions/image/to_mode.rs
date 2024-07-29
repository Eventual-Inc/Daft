use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::{functions::FunctionExpr, ExprRef};
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, ImageExpr};

pub struct ToModeEvaluator {}

impl FunctionEvaluator for ToModeEvaluator {
    fn fn_name(&self) -> &'static str {
        "to_mode"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        let mode = match expr {
            FunctionExpr::Image(ImageExpr::ToMode { mode }) => mode,
            _ => panic!("Expected ToMode Expr, got {expr}"),
        };
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                let output_dtype = match field.dtype {
                    DataType::Image(_) => DataType::Image(Some(*mode)),
                    DataType::FixedShapeImage(_, h, w) => DataType::FixedShapeImage(*mode, h, w),
                    _ => {
                        return Err(DaftError::TypeError(format!(
                        "ToMode can only operate on ImageArrays and FixedShapeImageArrays, got {}",
                        field
                    )))
                    }
                };
                Ok(Field::new(field.name, output_dtype))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let mode = match expr {
            FunctionExpr::Image(ImageExpr::ToMode { mode }) => mode,
            _ => panic!("Expected ToMode Expr, got {expr}"),
        };
        match inputs {
            [input] => input.image_to_mode(*mode),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
