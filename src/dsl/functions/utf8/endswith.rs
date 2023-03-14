use crate::{
    datatypes::{DataType, Field},
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::super::FunctionEvaluator;

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
                if !data_field.dtype.is_utf8() {
                    // TODO: Change to SchemaResolveTypeError
                    return Err(DaftError::TypeError(format!(
                        "Expected data input to endswith to be utf8, got {}",
                        data_field.dtype
                    )));
                }
                if !pattern_field.dtype.is_utf8() {
                    // TODO: Change to SchemaResolveTypeError
                    return Err(DaftError::TypeError(format!(
                        "Expected pattern input to endswith to be utf8, got {}",
                        pattern_field.dtype
                    )));
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
            [data, pattern] => Ok(data.utf8_endswith(pattern)?),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
