use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::functions::FunctionExpr;
use crate::ExprRef;

use super::super::FunctionEvaluator;

pub(super) enum LogFunction {
    Log2,
    Log10,
    Ln,
}
pub(super) struct LogEvaluator(pub LogFunction);

impl FunctionEvaluator for LogEvaluator {
    fn fn_name(&self) -> &'static str {
        match self.0 {
            LogFunction::Log2 => "log2",
            LogFunction::Log10 => "log10",
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
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to log to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
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
            LogFunction::Ln => input.ln(),
        }
    }
}
