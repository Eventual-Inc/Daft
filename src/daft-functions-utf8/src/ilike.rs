use ahash::AHashMap;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::{
    like::compile_like_regex,
    utils::{
        binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
    },
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ILike;

#[typetag::serde]
impl ScalarUDF for ILike {
    fn name(&self) -> &'static str {
        "ilike"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            s.with_utf8_array(|arr| {
                pattern
                    .with_utf8_array(|pattern_arr| Ok(ilike_impl(arr, pattern_arr)?.into_series()))
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
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::Boolean,
        )
    }

    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether string matches the given pattern. (case-insensitive)"
    }
}

#[must_use]
pub fn ilike(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFn::builtin(ILike {}, vec![input, pattern]).into()
}

fn ilike_impl(arr: &Utf8Array, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in ilike: {e}")))?;
    if is_full_null {
        return Ok(BooleanArray::full_null(
            arr.name(),
            &DataType::Boolean,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(BooleanArray::empty(arr.name(), &DataType::Boolean));
    }

    let self_iter = create_broadcasted_str_iter(arr, expected_size);

    let result_vec: Vec<Option<bool>> = if pattern.len() == 1 {
        let pat = pattern.get(0).unwrap();
        let re = compile_like_regex(pat, true)?;
        self_iter.map(|val| Some(re.is_match(val?))).collect()
    } else {
        let mut cache = AHashMap::new();
        let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
        self_iter
            .zip(pattern_iter)
            .map(|(val, pat)| match (val, pat) {
                (Some(val), Some(pat)) => {
                    let re = match cache.get(pat) {
                        Some(re) => re,
                        None => {
                            let compiled = compile_like_regex(pat, true)?;
                            cache.insert(pat.to_string(), compiled);
                            cache.get(pat).expect("regex inserted above")
                        }
                    };
                    Ok(Some(re.is_match(val)))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<Vec<Option<bool>>>>()?
    };

    let result = BooleanArray::from((arr.name(), result_vec.as_slice()));
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ilike_broadcast_pattern() -> DaftResult<()> {
        // Test case-insensitive matching with broadcast pattern
        let arr = Utf8Array::from_values("data", ["Hello", "HELLO", "hello", "World"].iter());
        let pattern = Utf8Array::from_values("pattern", ["hello"].iter());
        let result = ilike_impl(&arr, &pattern)?;

        assert_eq!(result.len(), 4);
        assert_eq!(result.get(0), Some(true)); // "Hello" ilike "hello"
        assert_eq!(result.get(1), Some(true)); // "HELLO" ilike "hello"
        assert_eq!(result.get(2), Some(true)); // "hello" ilike "hello"
        assert_eq!(result.get(3), Some(false)); // "World" ilike "hello"
        Ok(())
    }

    #[test]
    fn test_ilike_with_wildcards() -> DaftResult<()> {
        // Test with SQL LIKE wildcards (% and _)
        let arr = Utf8Array::from_values("data", ["Hello World", "HELLO", "hello there"].iter());
        let pattern = Utf8Array::from_values("pattern", ["hello%"].iter());
        let result = ilike_impl(&arr, &pattern)?;

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(true)); // "Hello World" ilike "hello%"
        assert_eq!(result.get(1), Some(true)); // "HELLO" ilike "hello%"
        assert_eq!(result.get(2), Some(true)); // "hello there" ilike "hello%"
        Ok(())
    }

    #[test]
    fn test_ilike_element_wise() -> DaftResult<()> {
        // Test element-wise matching with multiple patterns
        let arr = Utf8Array::from_values("data", ["Apple", "Banana", "Cherry"].iter());
        let pattern = Utf8Array::from_values("pattern", ["apple", "BERRY", "cherry"].iter());
        let result = ilike_impl(&arr, &pattern)?;

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(true)); // "Apple" ilike "apple"
        assert_eq!(result.get(1), Some(false)); // "Banana" ilike "BERRY"
        assert_eq!(result.get(2), Some(true)); // "Cherry" ilike "cherry"
        Ok(())
    }

    #[test]
    fn test_ilike_with_nulls() -> DaftResult<()> {
        // Test null handling - use collect() and rename() for arrays with nulls
        let arr: Utf8Array = [Some("Hello"), None, Some("World")].into_iter().collect();
        let arr = arr.rename("data");
        let pattern = Utf8Array::from_values("pattern", ["hello"].iter());
        let result = ilike_impl(&arr, &pattern)?;

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some(true)); // "Hello" ilike "hello"
        assert!(result.get(1).is_none()); // null ilike "hello" = null
        assert_eq!(result.get(2), Some(false)); // "World" ilike "hello"
        Ok(())
    }
}
