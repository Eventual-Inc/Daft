use crate::{functions::FunctionExpr, ExprRef};
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct SplitEvaluator {}

impl FunctionEvaluator for SplitEvaluator {
    fn fn_name(&self) -> &'static str {
        "split"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => match (data.to_field(schema), pattern.to_field(schema)) {
                (Ok(data_field), Ok(pattern_field)) => {
                    match (&data_field.dtype, &pattern_field.dtype) {
                        (DataType::Utf8, DataType::Utf8) => {
                            Ok(Field::new(data_field.name, DataType::List(Box::new(DataType::Utf8))))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expects inputs to split to be utf8, but received {data_field} and {pattern_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => {
                let regex = match expr {
                    FunctionExpr::Utf8(Utf8Expr::Split(regex)) => regex,
                    _ => panic!("Expected Utf8 Split Expr, got {expr}"),
                };
                data.utf8_split(pattern, *regex)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
