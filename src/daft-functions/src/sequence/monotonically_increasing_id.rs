use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

/// Marker trait for special functions that must be handled by the planner
pub trait SpecialFunction {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MonotonicallyIncreasingId {}

impl SpecialFunction for MonotonicallyIncreasingId {}

#[typetag::serde]
impl ScalarUDF for MonotonicallyIncreasingId {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "monotonically_increasing_id"
    }

    fn to_field(&self, inputs: &[ExprRef], _schema: &Schema) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::UInt64))
    }

    fn evaluate(&self, _inputs: &[Series]) -> DaftResult<Series> {
        Err(DaftError::NotImplemented(
            "monotonically_increasing_id is a special function that must be handled by the query planner".to_string(),
        ))
    }

    fn is_special(&self) -> bool {
        true
    }
}

#[must_use]
pub fn monotonically_increasing_id() -> ExprRef {
    ScalarFunction::new(MonotonicallyIncreasingId {}, vec![]).into()
}
