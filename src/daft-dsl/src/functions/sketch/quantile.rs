use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::DataType, datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::Expr;

pub(super) struct QuantileEvaluator {}

impl FunctionEvaluator for QuantileEvaluator {
    fn fn_name(&self) -> &'static str {
        "get"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if field.dtype != DataType::Binary {
            return Err(DaftError::TypeError(format!(
                "Expected input to floor to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(Field::new(field.name, DataType::Float64))
    }

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().approx_quantile()
    }
}
