use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use super::SketchExpr;
use crate::functions::FunctionExpr;
use crate::ExprRef;

pub(super) struct PercentileEvaluator {}

impl FunctionEvaluator for PercentileEvaluator {
    fn fn_name(&self) -> &'static str {
        "get"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match input_field.dtype {
                    DataType::Struct(_) => Ok(Field::new(
                        input_field.name,
                        DataType::List(Box::new(DataType::Float64)),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a struct type, received: {}",
                        input_field.dtype
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
        match inputs {
            [input] => match expr {
                FunctionExpr::Sketch(SketchExpr::Percentile(percentiles)) => {
                    input.sketch_percentile(percentiles.0.as_slice())
                }
                _ => unreachable!(
                    "PercentileEvaluator must evaluate a SketchExpr::Percentile expression"
                ),
            },
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
