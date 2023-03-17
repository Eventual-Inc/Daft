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
                if !matches!(data_field.dtype, DataType::Utf8)
                    || !matches!(pattern_field.dtype, DataType::Utf8)
                {
                    return Err(DaftError::TypeError(format!(
                        "Expects inputs to endwith to be utf8, but received {:?} and {:?}",
                        data_field, pattern_field
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
            [data, pattern] => data.utf8_endswith(pattern),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
