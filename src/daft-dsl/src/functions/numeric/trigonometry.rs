use common_error::{DaftError, DaftResult};
pub use daft_core::array::ops::trigonometry::TrigonometricFunction;
use daft_core::datatypes::Field;
use daft_core::schema::Schema;
use daft_core::{DataType, Series};

use crate::functions::{FunctionEvaluator, FunctionExpr};
use crate::ExprRef;

pub(super) struct TrigonometryEvaluator(pub TrigonometricFunction);

impl FunctionEvaluator for TrigonometryEvaluator {
    fn fn_name(&self) -> &'static str {
        self.0.fn_name()
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };
        let field = inputs.first().unwrap().to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to trigonometry to be numeric, got {}",
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
        inputs.first().unwrap().trigonometry(&self.0)
    }
}
