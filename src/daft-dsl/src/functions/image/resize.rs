use crate::functions::image::ImageExpr;
use crate::Expr;
use common_error::DaftError;
use daft_core::datatypes::DataType;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use common_error::DaftResult;

use super::super::FunctionEvaluator;

use super::super::FunctionExpr;

pub struct ResizeEvaluator {}

impl FunctionEvaluator for ResizeEvaluator {
    fn fn_name(&self) -> &'static str {
        "resize"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, expr: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;

                match &field.dtype {
                    DataType::Image(mode) => match mode {
                        Some(mode) => {
                            let (w, h) = match expr {
                                Expr::Function {
                                    func: FunctionExpr::Image(ImageExpr::Resize { w, h }),
                                    inputs: _,
                                } => (w, h),
                                _ => panic!("Expected ImageResize Expr, got {expr}"),
                            };
                            Ok(Field::new(
                                field.name,
                                DataType::FixedShapeImage(*mode, *h, *w),
                            ))
                        }
                        None => Ok(field.clone()),
                    },
                    DataType::FixedShapeImage(..) => Ok(field.clone()),
                    _ => Err(DaftError::TypeError(format!(
                        "ImageResize can only resize ImageArrays and FixedShapeImageArrays, got {}",
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
