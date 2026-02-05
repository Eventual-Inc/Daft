use std::sync::Arc;

use arrow_array::builder::LargeStringBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{
        FunctionArgs, ScalarUDF,
        scalar::{EvalContext, ScalarFn},
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid as RustUuid;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, _inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        Err(DaftError::ComputeError(
            "uuid() must be evaluated with execution context".to_string(),
        ))
    }

    fn call_with_ctx(
        &self,
        inputs: FunctionArgs<Series>,
        ctx: Option<&EvalContext>,
    ) -> DaftResult<Series> {
        let len = match inputs.len() {
            0 => ctx.map(|c| c.row_count).ok_or_else(|| {
                DaftError::ComputeError(
                    "uuid() requires evaluation context to determine output length".to_string(),
                )
            })?,
            1 => inputs.required(0)?.len(),
            n => {
                return Err(DaftError::ValueError(format!(
                    "Expected 0 args (planner) or 1 internal arg (evaluator), got {n}"
                )));
            }
        };

        const UUID_STR_LEN: usize = 36; // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        let mut builder = LargeStringBuilder::with_capacity(len, len * UUID_STR_LEN);
        for _ in 0..len {
            builder.append_value(RustUuid::new_v4().to_string());
        }
        let arrow_arr: arrow_array::ArrayRef = Arc::new(builder.finish());
        Series::from_arrow(Arc::new(Field::new("", DataType::Utf8)), arrow_arr)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUID strings."
    }
}

#[must_use]
pub fn uuid() -> ExprRef {
    ScalarFn::builtin(Uuid, vec![]).into()
}
