use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use common_error::{DaftError, DaftResult};
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

const UUID_LEN: i32 = 16;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let len = ctx.row_count;

        let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN);
        for _ in 0..len {
            builder.append_value(RustUuid::new_v4())?;
        }
        uuid_series_from_builder(builder)
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
        "Generates a column of UUIDv4 values."
    }
}

#[must_use]
pub fn uuid() -> ExprRef {
    ScalarFn::builtin(Uuid, vec![]).into()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UuidV7;

#[typetag::serde]
impl ScalarUDF for UuidV7 {
    fn name(&self) -> &'static str {
        "uuidv7"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["uuid_v7"]
    }

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let array = uuid_v7_kernel(ctx.row_count)?;
        uuid_series_from_builder(array)
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
        "Generates a column of UUIDv7 values."
    }
}

#[must_use]
pub fn uuidv7() -> ExprRef {
    ScalarFn::builtin(UuidV7, vec![]).into()
}

fn uuid_series_from_builder(mut builder: FixedSizeBinaryBuilder) -> DaftResult<Series> {
    Ok(
        UuidArray::from_arrow(Field::new("", DataType::Uuid), Arc::new(builder.finish()))?
            .into_series(),
    )
}

fn uuid_v7_kernel(len: usize) -> DaftResult<FixedSizeBinaryBuilder> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN);

    for _ in 0..len {
        builder.append_value(RustUuid::now_v7())?;
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    #[test]
    fn uuid_v7_kernel_sets_version_and_variant_bits() {
        let array = uuid_v7_kernel(128).unwrap().finish();

        assert_eq!(array.len(), 128);
        for idx in 0..array.len() {
            let bytes = array.value(idx);
            assert_eq!(bytes[6] >> 4, 0x7);
            assert_eq!(bytes[8] >> 6, 0b10);
        }
    }

    #[test]
    fn uuid_v7_kernel_outputs_lexicographically_ordered_values() {
        let array = uuid_v7_kernel(128).unwrap().finish();

        for idx in 1..array.len() {
            assert!(array.value(idx - 1) < array.value(idx));
        }
    }
}
