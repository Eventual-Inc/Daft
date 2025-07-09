use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, ScalarFunction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MonotonicallyIncreasingId;

#[derive(FunctionArgs)]
struct Args {}

#[typetag::serde]
impl ScalarUDF for MonotonicallyIncreasingId {
    fn name(&self) -> &'static str {
        "monotonically_increasing_id"
    }
    fn call(&self, _: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        Err(DaftError::NotImplemented(
            "monotonically_increasing_id should be rewritten into a separate plan step by the optimizer. If you're seeing this error, the DetectMonotonicId optimization rule may not have been applied.".to_string(),
        ))
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, _: &Schema) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::UInt64))
    }
}

#[must_use]
pub fn monotonically_increasing_id() -> ExprRef {
    ScalarFunction::new(MonotonicallyIncreasingId, vec![]).into()
}
