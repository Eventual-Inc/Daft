use crate::Expr;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct MaxEvaluator {}

impl FunctionEvaluator for MaxEvaluator {
    fn fn_name(&self) -> &'static str {
        "max"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
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

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_max()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
