use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field, TimeUnit},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct ToDatetimeEvaluator {}

impl FunctionEvaluator for ToDatetimeEvaluator {
    fn fn_name(&self) -> &'static str {
        "todatetime"
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
                        let timeunit = if format.contains("%9f") || format.contains("%.9f") {
                            TimeUnit::Nanoseconds
                        } else if format.contains("%3f") || format.contains("%.3f") {
                            TimeUnit::Milliseconds
                        } else {
                            TimeUnit::Microseconds
                        };
                        Ok(Field::new(
                            data_field.name,
                            DataType::Timestamp(timeunit, timezone.clone()),
                        ))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to todate to be utf8, but received {data_field}",
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
