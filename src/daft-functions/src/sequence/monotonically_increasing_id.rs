use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MonotonicallyIncreasingId {}

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
            "monotonically_increasing_id should be rewritten into a separate plan step by the optimizer. If you're seeing this error, the DetectMonotonicId optimization rule may not have been applied.".to_string(),
        ))
    }
}

#[must_use]
pub fn monotonically_increasing_id() -> ExprRef {
    ScalarFunction::new(MonotonicallyIncreasingId {}, vec![]).into()
}
