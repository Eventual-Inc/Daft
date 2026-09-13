use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FullNull, Int64Array, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Find;

#[typetag::serde]
impl ScalarUDF for Find {
    fn name(&self) -> &'static str {
        "find"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
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
        "Returns the 0-based character index of the first occurrence of the substring in each string, or -1 if not found."
    }
}

#[must_use]
pub fn find(input: ExprRef, substr: ExprRef) -> ExprRef {
    ScalarFn::builtin(Find {}, vec![input, substr]).into()
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
    let iter = self_iter
        .zip(substr_iter)
        .map(|(val, substr)| match (val, substr) {
            (Some(val), Some(substr)) => Some(find_char_index(val, substr)),
            _ => None,
        });

    let result = Int64Array::from_iter(Arc::new(Field::new(arr.name(), DataType::Int64)), iter);
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Returns the 0-based Unicode character index of `needle` in `haystack`, or -1 if not found.
///
/// Rust's [`str::find`] returns a byte offset, which disagrees with `substr` / `left` / `right`
/// (those operate on character indices). Empty needles match at index 0, matching `str::find`.
fn find_char_index(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 0;
    }
    haystack
        .char_indices()
        .enumerate()
        .find(|&(_, (byte_idx, _))| haystack[byte_idx..].starts_with(needle))
        .map(|(char_idx, _)| char_idx as i64)
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_char_index_ascii() {
        assert_eq!(find_char_index("foobar", "bar"), 3);
        assert_eq!(find_char_index("foobar", "baz"), -1);
        assert_eq!(find_char_index("foobar", ""), 0);
        assert_eq!(find_char_index("", "a"), -1);
        assert_eq!(find_char_index("", ""), 0);
    }

    #[test]
    fn test_find_char_index_unicode() {
        assert_eq!(find_char_index("你好世界", "世"), 2);
        assert_eq!(find_char_index("你好世界", "好"), 1);
        assert_eq!(find_char_index("你好世界", "界外"), -1);
        assert_eq!(find_char_index("a😀b", "b"), 2);
        assert_eq!(find_char_index("a😀b", "😀"), 1);
    }
}
