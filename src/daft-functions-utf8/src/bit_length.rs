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
pub struct BitLength;

#[typetag::serde]
impl ScalarUDF for BitLength {
    fn name(&self) -> &'static str {
        "bit_length"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| Ok(bit_length_impl(arr)?.into_series()))
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
        "Returns the length of the string in bits"
    }
}

#[must_use]
pub fn utf8_bit_length(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(BitLength {}, vec![input]).into()
}

fn bit_length_impl(arr: &Utf8Array) -> DaftResult<UInt64Array> {
    let iter = arr.into_iter().map(|val| {
        let v = val?;
        Some((v.len() as u64) * 8)
    });

    Ok(UInt64Array::from_iter(
        Arc::new(Field::new(arr.name(), DataType::UInt64)),
        iter,
    ))
}
