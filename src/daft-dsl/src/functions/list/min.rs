use crate::ExprRef;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct MinEvaluator {}

impl FunctionEvaluator for MinEvaluator {
    fn fn_name(&self) -> &'static str {
        "min"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?.to_exploded_field()?;

                if field.dtype.is_numeric() {
                    Ok(field)
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expected input to be numeric, got {}",
                        field.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_min()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
