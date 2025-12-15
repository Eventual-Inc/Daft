use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LengthBytes;

#[typetag::serde]
impl ScalarUDF for LengthBytes {
    fn name(&self) -> &'static str {
        "length_bytes"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| Ok(length_bytes_impl(arr)?.into_series()))
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::UInt64)
    }
    fn docstring(&self) -> &'static str {
        "Returns the length of the string in bytes"
    }
}

#[must_use]
pub fn utf8_length_bytes(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(LengthBytes {}, vec![input]).into()
}

fn length_bytes_impl(arr: &Utf8Array) -> DaftResult<UInt64Array> {
    let iter = arr.into_iter().map(|val| {
        let v = val?;
        Some(v.len() as u64)
    });

    Ok(UInt64Array::from_iter(
        Arc::new(Field::new(arr.name(), DataType::UInt64)),
        iter,
    ))
}
