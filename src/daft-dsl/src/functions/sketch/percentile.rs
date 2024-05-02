use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use super::SketchExpr;
use crate::functions::FunctionExpr;
use crate::ExprRef;

pub(super) struct PercentileEvaluator {}

impl FunctionEvaluator for PercentileEvaluator {
    fn fn_name(&self) -> &'static str {
        "sketch_percentile"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match (input_field.dtype, expr) {
                    (
                        DataType::Struct(_),
                        FunctionExpr::Sketch(SketchExpr::Percentile {
                            percentiles,
                            force_list_output,
                        }),
                    ) => Ok(Field::new(
                        input_field.name,
                        if percentiles.0.len() > 1 || *force_list_output {
                            DataType::FixedSizeList(
                                Box::new(DataType::Float64),
                                percentiles.0.len(),
                            )
                        } else {
                            DataType::Float64
                        },
                    )),
                    (input_field_dtype, FunctionExpr::Sketch(SketchExpr::Percentile { .. })) => {
                        Err(DaftError::TypeError(format!(
                            "Expected input to be a struct type, received: {}",
                            input_field_dtype
                        )))
                    }
                    (_, _) => Err(DaftError::TypeError(format!(
                        "Expected expr to be a Sketch Percentile expression, received: {}",
                        expr
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
                FunctionExpr::Sketch(SketchExpr::Percentile {
                    percentiles,
                    force_list_output,
                }) => input.sketch_percentile(percentiles.0.as_slice(), *force_list_output),
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
