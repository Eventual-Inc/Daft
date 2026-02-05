use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
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

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let len = match inputs.len() {
            0 => {
                return Err(DaftError::ComputeError(
                    "uuid() requires evaluation context to determine output length".to_string(),
                ));
            }
            1 => inputs.required(0)?.len(),
            n => {
                return Err(DaftError::ValueError(format!(
                    "Expected 0 args (planner) or 1 internal arg (evaluator), got {n}"
                )));
            }
        };

        let iter = (0..len).map(|_| Some(RustUuid::new_v4().to_string()));
        let arr = Utf8Array::from_iter("", iter);
        Ok(arr.into_series())
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
