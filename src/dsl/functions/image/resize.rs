use crate::datatypes::DataType;
use crate::dsl::functions::image::ImageExpr;
use crate::error::DaftError;
use crate::{datatypes::Field, dsl::Expr, error::DaftResult, schema::Schema, series::Series};

use super::super::FunctionEvaluator;

use super::super::FunctionExpr;

pub struct ResizeEvaluator {}

impl FunctionEvaluator for ResizeEvaluator {
    fn fn_name(&self) -> &'static str {
        "resize"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;

                match &field.dtype {
                    DataType::Image(_, _) => Ok(field.clone()),
                    _ => Err(DaftError::TypeError(format!(
                        "ImageResize can only resize ImageArrays, got {}",
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

    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series> {
        let (w, h) = match expr {
            Expr::Function {
                func: FunctionExpr::Image(ImageExpr::Resize { w, h }),
                inputs: _,
            } => (w, h),
            _ => panic!("Expected ImageResize Expr, got {expr}"),
        };

        match inputs {
            [input] => input.image_resize(*w, *h),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
