use crate::Expr;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct GetEvaluator {}

impl FunctionEvaluator for GetEvaluator {
    fn fn_name(&self) -> &'static str {
        "get"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input, idx, default] => {
                let input_field = input.to_field(schema)?;
                let idx_field = idx.to_field(schema)?;
                let _default_field = default.to_field(schema)?;

                if !idx_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected get index to be integer, received: {}",
                        idx_field.dtype
                    )));
                }

                // TODO(Kevin): Check if default dtype can be cast into input dtype.

                let exploded_field = input_field.to_exploded_field()?;
                Ok(exploded_field)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input, idx, default] => Ok(input.list_get(idx, default)?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
