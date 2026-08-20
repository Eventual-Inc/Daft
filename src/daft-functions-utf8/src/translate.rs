use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

/// Spark-compatible `translate` function.
///
/// Translates characters in `input` that exist in `from` to the corresponding characters in `to`.
/// Characters in `from` that don't have a corresponding character in `to` are removed.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Translate;

#[typetag::serde]
impl ScalarUDF for Translate {
    fn name(&self) -> &'static str {
        "translate"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let from = inputs.required((1, "from"))?;
        let to = inputs.required((2, "to"))?;

        input.with_utf8_array(|arr| {
            from.with_utf8_array(|from_arr| {
                to.with_utf8_array(
                    |to_arr| Ok(translate_impl(arr, from_arr, to_arr)?.into_series()),
                )
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 3, "translate expects 3 arguments");
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let from = inputs.required((1, "from"))?.to_field(schema)?;
        let to = inputs.required((2, "to"))?.to_field(schema)?;
        ensure!(
            input.dtype.is_string(), TypeError: "Input must be of type Utf8"
        );
        ensure!(
            from.dtype.is_string(), TypeError: "'from' must be of type Utf8"
        );
        ensure!(
            to.dtype.is_string(), TypeError: "'to' must be of type Utf8"
        );
        Ok(input)
    }

    fn docstring(&self) -> &'static str {
        "Translates characters in the input string by replacing characters in 'from' with corresponding characters in 'to'. Characters in 'from' without a corresponding character in 'to' are removed."
    }
}

#[must_use]
pub fn translate(input: ExprRef, from: ExprRef, to: ExprRef) -> ExprRef {
    ScalarFn::builtin(Translate {}, vec![input, from, to]).into()
}

fn translate_impl(
    arr: &Utf8Array,
    from_arr: &Utf8Array,
    to_arr: &Utf8Array,
) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[from_arr, to_arr])
        .map_err(|e| DaftError::ValueError(format!("Error in translate: {e}")))?;
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
    let from_iter = create_broadcasted_str_iter(from_arr, expected_size);
    let to_iter = create_broadcasted_str_iter(to_arr, expected_size);

    let result: Utf8Array = arr_iter
        .zip(from_iter)
        .zip(to_iter)
        .map(|((val, from), to)| match (val, from, to) {
            (Some(val), Some(from), Some(to)) => Some(translate_string(val, from, to)),
            _ => None,
        })
        .collect::<Utf8Array>()
        .rename(arr.name());

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Translate characters in `input` based on `from` -> `to` mapping.
/// Characters in `from` that have a corresponding character in `to` are replaced.
/// Characters in `from` without a corresponding character in `to` are removed.
fn translate_string(input: &str, from: &str, to: &str) -> String {
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();

    input
        .chars()
        .filter_map(|c| {
            if let Some(pos) = from_chars.iter().position(|&fc| fc == c) {
                if pos < to_chars.len() {
                    Some(to_chars[pos])
                } else {
                    // Character in 'from' but no corresponding in 'to' -> remove
                    None
                }
            } else {
                // Character not in 'from' -> keep as is
                Some(c)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_translate_basic() {
        let arr = Utf8Array::from_iter("a", vec![Some("AaBbCc")].into_iter());
        let from = Utf8Array::from_iter("from", vec![Some("abc")].into_iter());
        let to = Utf8Array::from_iter("to", vec![Some("123")].into_iter());

        let result = translate_impl(&arr, &from, &to).unwrap();
        assert_eq!(result.get(0), Some("A1B2C3"));
    }

    #[test]
    fn test_translate_remove_chars() {
        let arr = Utf8Array::from_iter("a", vec![Some("hello")].into_iter());
        let from = Utf8Array::from_iter("from", vec![Some("elo")].into_iter());
        let to = Utf8Array::from_iter("to", vec![Some("E")].into_iter());

        let result = translate_impl(&arr, &from, &to).unwrap();
        // 'e' -> 'E', 'l' -> removed, 'o' -> removed
        assert_eq!(result.get(0), Some("hE"));
    }

    #[test]
    fn test_translate_with_nulls() {
        let arr = Utf8Array::from_iter("a", vec![Some("hello"), None, Some("world")].into_iter());
        let from = Utf8Array::from_iter("from", vec![Some("lo")].into_iter());
        let to = Utf8Array::from_iter("to", vec![Some("LO")].into_iter());

        let result = translate_impl(&arr, &from, &to).unwrap();
        assert_eq!(result.get(0), Some("heLLO"));
        assert_eq!(result.get(1), None);
        assert_eq!(result.get(2), Some("wOrLd"));
    }

    #[test]
    fn test_translate_empty_from() {
        let arr = Utf8Array::from_iter("a", vec![Some("hello")].into_iter());
        let from = Utf8Array::from_iter("from", vec![Some("")].into_iter());
        let to = Utf8Array::from_iter("to", vec![Some("xyz")].into_iter());

        let result = translate_impl(&arr, &from, &to).unwrap();
        assert_eq!(result.get(0), Some("hello"));
    }
}
