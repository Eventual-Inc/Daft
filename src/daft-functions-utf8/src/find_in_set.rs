use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::DataArray,
    datatypes::Int32Type,
    prelude::{DataType, Field, FullNull, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

/// Compute Spark-compatible `find_in_set(str, str_array)`:
/// Returns the 1-based index of `str` in the comma-separated `str_array`,
/// or 0 if not found, or 0 if `str` contains a comma.
fn compute_find_in_set(needle: &str, haystack: &str) -> i32 {
    if needle.contains(',') {
        return 0;
    }
    for (idx, part) in haystack.split(',').enumerate() {
        if part == needle {
            return (idx as i32) + 1;
        }
    }
    0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FindInSet;

#[typetag::serde]
impl ScalarUDF for FindInSet {
    fn name(&self) -> &'static str {
        "find_in_set"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let needle = inputs.required((0, "str"))?.cast(&DataType::Utf8)?;
        let haystack = inputs.required((1, "str_array"))?.cast(&DataType::Utf8)?;
        let name = needle.name();

        needle.with_utf8_array(|needle_arr| {
            haystack.with_utf8_array(|haystack_arr| {
                let (is_full_null, expected_size) = parse_inputs(needle_arr, &[haystack_arr])
                    .map_err(|e| DaftError::ValueError(format!("Error in find_in_set: {e}")))?;

                if is_full_null {
                    return Ok(DataArray::<Int32Type>::full_null(
                        name,
                        &DataType::Int32,
                        expected_size,
                    )
                    .into_series());
                }
                if expected_size == 0 {
                    return Ok(DataArray::<Int32Type>::empty(name, &DataType::Int32).into_series());
                }

                let needle_iter = create_broadcasted_str_iter(needle_arr, expected_size);
                let haystack_iter = create_broadcasted_str_iter(haystack_arr, expected_size);

                let result: DataArray<Int32Type> = needle_iter
                    .zip(haystack_iter)
                    .map(|(n, h)| match (n, h) {
                        (Some(n), Some(h)) => Some(compute_find_in_set(n, h)),
                        _ => None,
                    })
                    .collect();

                Ok(result.rename(name).into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 inputs, but received {}",
            inputs.len()
        );

        let needle = inputs.required((0, "str"))?.to_field(schema)?;
        let haystack = inputs.required((1, "str_array"))?.to_field(schema)?;

        ensure!(
            needle.dtype.is_string() || needle.dtype == DataType::Null,
            TypeError: "First argument to 'find_in_set' must be a string, got {}",
            needle.dtype
        );
        ensure!(
            haystack.dtype.is_string() || haystack.dtype == DataType::Null,
            TypeError: "Second argument to 'find_in_set' must be a string, got {}",
            haystack.dtype
        );

        Ok(Field::new(needle.name, DataType::Int32))
    }

    fn docstring(&self) -> &'static str {
        "Returns the 1-based index of the first argument in the comma-separated second argument. \
        Returns 0 if not found, or 0 if the first argument contains a comma. \
        Returns null if either input is null."
    }
}

#[must_use]
pub fn find_in_set(needle: ExprRef, haystack: ExprRef) -> ExprRef {
    ScalarFn::builtin(FindInSet, vec![needle, haystack]).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_in_set_basic() {
        assert_eq!(compute_find_in_set("ab", "abc,b,ab,c,def"), 3);
        assert_eq!(compute_find_in_set("a", "a,b,c"), 1);
        assert_eq!(compute_find_in_set("c", "a,b,c"), 3);
        assert_eq!(compute_find_in_set("d", "a,b,c"), 0);
    }

    #[test]
    fn test_find_in_set_empty() {
        assert_eq!(compute_find_in_set("", ""), 1);
        assert_eq!(compute_find_in_set("", "a,,b"), 2);
        assert_eq!(compute_find_in_set("a", ""), 0);
    }

    #[test]
    fn test_find_in_set_with_comma_in_needle() {
        // Needles containing comma always return 0 per Spark spec.
        assert_eq!(compute_find_in_set("a,b", "a,b,c"), 0);
    }

    #[test]
    fn test_find_in_set_unicode() {
        assert_eq!(compute_find_in_set("β", "α,β,γ"), 2);
    }
}
