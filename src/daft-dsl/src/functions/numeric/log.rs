use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series, DataType};

use crate::functions::FunctionExpr;
use crate::ExprRef;

use super::super::FunctionEvaluator;
use super::NumericExpr;

pub(super) enum LogFunction {
    Log2,
    Log10,
    Log,
    Ln,
}
pub(super) struct LogEvaluator(pub LogFunction);

impl FunctionEvaluator for LogEvaluator {
    fn fn_name(&self) -> &'static str {
        match self.0 {
            LogFunction::Log2 => "log2",
            LogFunction::Log10 => "log10",
            LogFunction::Log => "log",
            LogFunction::Ln => "ln",
        }
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
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to log to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let input = inputs.first().unwrap();
        match self.0 {
            LogFunction::Log2 => input.log2(),
            LogFunction::Log10 => input.log10(),
            LogFunction::Log => {
                let base = match expr {
                    FunctionExpr::Numeric(NumericExpr::Log(value)) => value,
                    _ => panic!("Expected Log Expr, got {expr}"),
                };

                input.log(base.0)
            }
            LogFunction::Ln => input.ln(),
        }
    }
}
