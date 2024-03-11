use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, JsonExpr};

pub(super) struct JsonQueryEvaluator {}

impl FunctionEvaluator for JsonQueryEvaluator {
    fn fn_name(&self) -> &'static str {
        "JsonQuery"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match input_field.dtype {
                    DataType::Utf8 => Ok(Field::new(input_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a string type, received: {}",
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

    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => {
                let query = match expr {
                    Expr::Function {
                        func: FunctionExpr::Json(JsonExpr::Query(query)),
                        inputs: _,
                    } => query,
                    _ => panic!("Expected Json Query Expr, got {expr}"),
                };

                input.json_query(query)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
