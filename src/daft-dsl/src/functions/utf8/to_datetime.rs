use crate::functions::FunctionExpr;
use crate::ExprRef;
use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{infer_timeunit_from_format_string, DataType, Field},
    schema::Schema,
    series::Series,
};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct ToDatetimeEvaluator {}

impl FunctionEvaluator for ToDatetimeEvaluator {
    fn fn_name(&self) -> &'static str {
        "to_datetime"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => {
                        let (format, timezone) = match expr {
                            FunctionExpr::Utf8(Utf8Expr::ToDatetime(format, timezone)) => {
                                (format, timezone)
                            }
                            _ => panic!("Expected Utf8 ToDatetime Expr, got {expr}"),
                        };
                        let timeunit = infer_timeunit_from_format_string(format);
                        Ok(Field::new(
                            data_field.name,
                            DataType::Timestamp(timeunit, timezone.clone()),
                        ))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to to_datetime to be utf8, but received {data_field}",
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
                let (format, timezone) = match expr {
                    FunctionExpr::Utf8(Utf8Expr::ToDatetime(format, timezone)) => {
                        (format, timezone)
                    }
                    _ => panic!("Expected Utf8 ToDatetime Expr, got {expr}"),
                };
                data.utf8_to_datetime(format, timezone.as_deref())
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
