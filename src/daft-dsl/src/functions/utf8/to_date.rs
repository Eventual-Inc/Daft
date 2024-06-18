use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct ToDateEvaluator {}

impl FunctionEvaluator for ToDateEvaluator {
    fn fn_name(&self) -> &'static str {
        "to_date"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Date)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to to_date to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => {
                let format = match expr {
                    FunctionExpr::Utf8(Utf8Expr::ToDate(format)) => format,
                    _ => panic!("Expected Utf8 ToDate Expr, got {expr}"),
                };
                data.utf8_to_date(format)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
