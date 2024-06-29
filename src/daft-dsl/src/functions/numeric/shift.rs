use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::functions::FunctionExpr;
use crate::ExprRef;

use super::super::FunctionEvaluator;

pub(super) enum ShiftFunction {
    Right,
    Left,
}

pub(super) struct ShiftEvaluator(pub ShiftFunction);

impl FunctionEvaluator for ShiftEvaluator {
    fn fn_name(&self) -> &'static str {
        match self.0 {
            ShiftFunction::Right => "shift_right",
            ShiftFunction::Left => "shift_left",
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 2 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to shift to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 2 {
            return Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        }
        let input = inputs.first().unwrap();
        let shift = inputs.last().unwrap();
        match self.0 {
            ShiftFunction::Right => input.shift_right(shift),
            ShiftFunction::Left => input.shift_left(shift),
        }
    }
}
