use crate::{functions::FunctionExpr, Expr};
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, ListExpr};

pub(super) struct CountEvaluator {}

impl FunctionEvaluator for CountEvaluator {
    fn fn_name(&self) -> &'static str {
        "count"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, expr: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;

                match input_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => match expr {
                        Expr::Function {
                            func: FunctionExpr::List(ListExpr::Count(_)),
                            inputs: _,
                        } => Ok(Field::new(input.name()?, DataType::UInt64)),
                        _ => panic!("Expected List Count Expr, got {expr}"),
                    },
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a list type, received: {}",
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
                let mode = match expr {
                    Expr::Function {
                        func: FunctionExpr::List(ListExpr::Count(mode)),
                        inputs: _,
                    } => mode,
                    _ => panic!("Expected List Count Expr, got {expr}"),
                };

                Ok(input.list_count(*mode)?)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
