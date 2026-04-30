use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use daft_common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FromArrow, Schema, UuidArray},
    series::{IntoSeries, Series},
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

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let len = ctx.row_count;

        let mut builder = FixedSizeBinaryBuilder::with_capacity(len, 16);
        for _ in 0..len {
            builder.append_value(RustUuid::new_v4())?;
        }
        Ok(
            UuidArray::from_arrow(Field::new("", DataType::Uuid), Arc::new(builder.finish()))?
                .into_series(),
        )
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
        Ok(Field::new("", DataType::Uuid))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUID strings."
    }
}

#[must_use]
pub fn uuid() -> ExprRef {
    ScalarFn::builtin(Uuid, vec![]).into()
}
