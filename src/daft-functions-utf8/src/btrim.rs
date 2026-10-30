use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FullNull, Schema, Utf8Array},
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
pub struct BTrim;

#[typetag::serde]
impl ScalarUDF for BTrim {
    fn name(&self) -> &'static str {
        "btrim"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "trim_str", |s, trim_str| {
            s.with_utf8_array(|arr| {
                trim_str.with_utf8_array(|trim_arr| Ok(btrim_impl(arr, trim_arr)?.into_series()))
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
            "trim_str",
            DataType::is_string,
            self.name(),
            DataType::Utf8,
        )
    }

    fn docstring(&self) -> &'static str {
        "Removes leading and trailing characters that appear in the given set of characters"
    }
}

#[must_use]
pub fn btrim(input: ExprRef, trim_str: ExprRef) -> ExprRef {
    ScalarFn::builtin(BTrim, vec![input, trim_str]).into()
}

fn btrim_impl(arr: &Utf8Array, trim_str: &Utf8Array) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[trim_str])
        .map_err(|e| DaftError::ValueError(format!("Error in btrim: {e}")))?;
    if is_full_null {
        return Ok(Utf8Array::full_null(
            arr.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Utf8Array::empty(arr.name(), &DataType::Utf8));
    }

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    let trim_iter = create_broadcasted_str_iter(trim_str, expected_size);

    let result = arr_iter
        .zip(trim_iter)
        .map(|(val, trim)| match (val, trim) {
            (Some(val), Some(trim)) if !trim.is_empty() => {
                Ok(Some(val.trim_matches(|c| trim.contains(c))))
            }
            (Some(val), Some(_)) => Ok(Some(val)),
            _ => Ok(None),
        })
        .collect::<DaftResult<Utf8Array>>()?;

    Ok(result.rename(arr.name()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btrim_trims_character_set() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("xyxtrimyyx"), Some("  spaced  "), None, Some("abc")].into_iter(),
        );
        let trim = Utf8Array::from_iter("t", vec![Some("xy")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("trim"));
        assert_eq!(result.get(1), Some("  spaced  "));
        assert_eq!(result.get(2), None);
        assert_eq!(result.get(3), Some("abc"));
    }

    #[test]
    fn test_btrim_keeps_all_characters() {
        let arr = Utf8Array::from_iter("a", vec![Some("xxabcxx")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("x")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("abc"));
    }

    #[test]
    fn test_btrim_empty_trim_str_is_noop() {
        let arr = Utf8Array::from_iter("a", vec![Some("  space  ")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("  space  "));
    }

    #[test]
    fn test_btrim_all_trim_characters() {
        let arr = Utf8Array::from_iter("a", vec![Some("xxxx"), Some("")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("x")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some(""));
        assert_eq!(result.get(1), Some(""));
    }

    #[test]
    fn test_btrim_is_unicode_aware() {
        let arr = Utf8Array::from_iter("a", vec![Some("😃😄hello😄😃")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("😃😄")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("hello"));
    }

    #[test]
    fn test_btrim_broadcast_trim_str() {
        let arr =
            Utf8Array::from_iter("a", vec![Some("xax"), Some("yby"), Some("xcx")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("xy")].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("a"));
        assert_eq!(result.get(1), Some("b"));
        assert_eq!(result.get(2), Some("c"));
    }

    #[test]
    fn test_btrim_null_trim_str_row() {
        let arr = Utf8Array::from_iter("a", vec![Some("xax"), Some("yby")].into_iter());
        let trim = Utf8Array::from_iter("t", vec![Some("x"), None].into_iter());

        let result = btrim_impl(&arr, &trim).unwrap();

        assert_eq!(result.get(0), Some("a"));
        assert_eq!(result.get(1), None);
    }
}
