use crate::{
    datatypes::{DataType, Field},
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::{super::FunctionEvaluator, super::FunctionExpr, Utf8Expr};
use std::sync::Arc;
pub(super) struct EndswithEvaluator {}

impl FunctionEvaluator for EndswithEvaluator {
    fn fn_name(&self) -> &'static str {
        "endswith"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, pattern] => {
                let data_field = data.to_field(schema)?;
                let pattern_field = pattern.to_field(schema)?;
                if !matches!(data_field.dtype, DataType::Utf8)
                    || !matches!(pattern_field.dtype, DataType::Utf8)
                {
                    return Err(DaftError::ExprResolveTypeError {
                        expectation: "data input to endswith to be utf8".into(),
                        expr: Arc::new(Expr::Function {
                            func: FunctionExpr::Utf8(Utf8Expr::EndsWith),
                            inputs: inputs.to_vec(),
                        }),
                        child_fields_to_expr: vec![
                            (data_field, Arc::new(data.clone())),
                            (pattern_field, Arc::new(pattern.clone())),
                        ],
                    });
                }
                Ok(Field::new(data_field.name, DataType::Boolean))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, pattern] => data.utf8_endswith(pattern),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
