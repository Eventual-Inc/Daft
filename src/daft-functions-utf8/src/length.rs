use common_error::DaftResult;
use daft_core::{
    prelude::{AsArrow, DataType, Field, Schema, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Length;

#[typetag::serde]
impl ScalarUDF for Length {
    fn name(&self) -> &'static str {
        "length"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| Ok(length_impl(arr)?.into_series()))
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
        "Returns the length of the string"
    }
}

#[must_use]
pub fn length(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Length, vec![input]).into()
}

fn length_impl(arr: &Utf8Array) -> DaftResult<UInt64Array> {
    let self_arrow = arr.as_arrow();
    let arrow_result = self_arrow
        .iter()
        .map(|val| {
            let v = val?;
            Some(v.chars().count() as u64)
        })
        .collect::<arrow2::array::UInt64Array>()
        .with_validity(self_arrow.validity().cloned());
    Ok(UInt64Array::from((arr.name(), Box::new(arrow_result))))
}
