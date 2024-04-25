use common_error::{DaftError, DaftResult};
use daft_core::datatypes::Float64Array;
use daft_core::IntoSeries;
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
            [input] => {
                let percentiles_series = match expr {
                    FunctionExpr::Sketch(SketchExpr::Percentile(percentiles)) => {
                        Float64Array::from((
                            "percentiles",
                            percentiles
                                .iter()
                                .map(|&b| f64::from_be_bytes(b))
                                .collect::<Vec<_>>()
                                .as_slice(),
                        ))
                        .into_series()
                    }
                    _ => unreachable!(
                        "PercentileEvaluator must evaluate a SketchExpr::Percentile expression"
                    ),
                };
                input.sketch_percentile(&percentiles_series)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
