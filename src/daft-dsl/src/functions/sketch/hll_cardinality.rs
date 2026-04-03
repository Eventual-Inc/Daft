use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::super::FunctionEvaluator;
use crate::{ExprRef, functions::FunctionExpr};

pub(super) struct HllCardinalityEvaluator {}

impl FunctionEvaluator for HllCardinalityEvaluator {
    fn fn_name(&self) -> &'static str {
        "hll_cardinality"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        _expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match input_field.dtype {
                    DataType::FixedSizeBinary(_) => {
                        Ok(Field::new(input_field.name, DataType::UInt64))
                    }
                    other => Err(DaftError::TypeError(format!(
                        "Expected FixedSizeBinary input for hll_cardinality, got: {}",
                        other
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input] => input.hll_cardinality(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
