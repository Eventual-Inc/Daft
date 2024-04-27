use common_error::{DaftError, DaftResult};
use daft_core::DataType;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use crate::ExprRef;

use crate::functions::FunctionExpr;

pub(super) struct SqrtEvaluator {}

impl FunctionEvaluator for SqrtEvaluator {
    fn fn_name(&self) -> &'static str {
        "sqrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Int8 => DataType::Float32,
            DataType::Int16 => DataType::Float32,
            DataType::UInt8 => DataType::Float32,
            DataType::UInt16 => DataType::Float32,
            DataType::Int32 => DataType::Float64,
            DataType::Int64 => DataType::Float64,
            DataType::UInt32 => DataType::Float64,
            DataType::UInt64 => DataType::Float64,
            DataType::Float32 => DataType::Float32,
            DataType::Float64 => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to sqrt to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }
    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        inputs.first().unwrap().sqrt()
    }
}
