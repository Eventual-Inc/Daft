use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FullNull, Int64Array, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Find;

#[typetag::serde]
impl ScalarUDF for Find {
    fn name(&self) -> &'static str {
        "find"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "substr", |s, substr| {
            s.with_utf8_array(|arr| {
                substr.with_utf8_array(|substr_arr| {
                    find_impl(arr, substr_arr).map(IntoSeries::into_series)
                })
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "substr",
            DataType::is_string,
            self.name(),
            DataType::Int64,
        )
    }

    fn docstring(&self) -> &'static str {
        "Returns the index of the first occurrence of the substring in each string."
    }
}

#[must_use]
pub fn find(input: ExprRef, substr: ExprRef) -> ExprRef {
    ScalarFunction::new(Find {}, vec![input, substr]).into()
}

fn find_impl(arr: &Utf8Array, substr: &Utf8Array) -> DaftResult<Int64Array> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[substr])
        .map_err(|e| DaftError::ValueError(format!("Error in find: {e}")))?;
    if is_full_null {
        return Ok(Int64Array::full_null(
            arr.name(),
            &DataType::Int64,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Int64Array::empty(arr.name(), &DataType::Int64));
    }

    let self_iter = create_broadcasted_str_iter(arr, expected_size);
    let substr_iter = create_broadcasted_str_iter(substr, expected_size);
    let arrow_result = self_iter
        .zip(substr_iter)
        .map(|(val, substr)| match (val, substr) {
            (Some(val), Some(substr)) => Some(val.find(substr).map(|pos| pos as i64).unwrap_or(-1)),
            _ => None,
        })
        .collect::<arrow2::array::Int64Array>();

    let result = Int64Array::from((arr.name(), Box::new(arrow_result)));
    assert_eq!(result.len(), expected_size);
    Ok(result)
}
