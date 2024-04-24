use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::ExprRef;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct ReplaceEvaluator {}

impl FunctionEvaluator for ReplaceEvaluator {
    fn fn_name(&self) -> &'static str {
        "replace"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, pattern, replacement] => match (
                data.to_field(schema),
                pattern.to_field(schema),
                replacement.to_field(schema),
            ) {
                (Ok(data_field), Ok(pattern_field), Ok(replacement_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype, &replacement_field.dtype) {
                        (DataType::Utf8, DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::Utf8))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to Replace to be utf8, but received {data_field} and {pattern_field} and {replacement_field}",
                        ))),
                    }
                }
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, pattern, replacement] => {
                let regex = match expr {
                    FunctionExpr::Utf8(Utf8Expr::Replace(regex)) => regex,
                    _ => panic!("Expected Utf8 Replace Expr, got {expr}"),
                };
                data.utf8_replace(pattern, replacement, *regex)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
