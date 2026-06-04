use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::DataArray,
    prelude::{DaftIntegerType, DaftNumericType, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::NumCast;
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

/// Spark-compatible `substring_index` function.
///
/// Returns the substring from string `str` before `count` occurrences of the delimiter `delim`.
/// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
/// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SubstringIndex;

#[typetag::serde]
impl ScalarUDF for SubstringIndex {
    fn name(&self) -> &'static str {
        "substring_index"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let delim = inputs.required((1, "delim"))?;
        let count = inputs.required((2, "count"))?;

        input.with_utf8_array(|arr| {
            delim.with_utf8_array(|delim_arr| {
                if count.data_type().is_integer() {
                    with_match_integer_daft_types!(count.data_type(), |$T| {
                        Ok(substring_index_impl(arr, delim_arr, count.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                    })
                } else if count.data_type().is_null() {
                    Ok(Series::full_null(input.name(), &DataType::Utf8, input.len()))
                } else {
                    Err(DaftError::TypeError(format!(
                        "substring_index not implemented for count type {}",
                        count.data_type()
                    )))
                }
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 3, "substring_index expects 3 arguments");
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let delim = inputs.required((1, "delim"))?.to_field(schema)?;
        let count = inputs.required((2, "count"))?.to_field(schema)?;
        ensure!(
            input.dtype.is_string(), TypeError: "Input must be of type Utf8"
        );
        ensure!(
            delim.dtype.is_string(), TypeError: "Delimiter must be of type Utf8"
        );
        ensure!(
            count.dtype.is_integer(), TypeError: "Count must be of integer type, got {}", count.dtype
        );
        Ok(input)
    }

    fn docstring(&self) -> &'static str {
        "Returns the substring from string before count occurrences of the delimiter. If count is positive, returns everything to the left of the final delimiter (counting from left). If count is negative, returns everything to the right of the final delimiter (counting from right)."
    }
}

#[must_use]
pub fn substring_index(input: ExprRef, delim: ExprRef, count: ExprRef) -> ExprRef {
    ScalarFn::builtin(SubstringIndex {}, vec![input, delim, count]).into()
}

fn substring_index_impl<I>(
    arr: &Utf8Array,
    delim_arr: &Utf8Array,
    count: &DataArray<I>,
) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord + std::hash::Hash,
{
    let (is_full_null, expected_size) = parse_inputs(arr, &[delim_arr])
        .map_err(|e| DaftError::ValueError(format!("Error in substring_index: {e}")))?;
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

    ensure!(
        count.len() == 1 || count.len() == expected_size,
        ComputeError: "substring_index: count array length ({}) is not broadcastable to expected size ({})",
        count.len(),
        expected_size
    );

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    let delim_iter = create_broadcasted_str_iter(delim_arr, expected_size);

    let result: Utf8Array = match count.len() {
        1 => match count.get(0) {
            Some(n) => {
                let n: i64 = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(
                        "Error in substring_index: failed to cast count as i64".to_string(),
                    )
                })?;
                arr_iter
                    .zip(delim_iter)
                    .map(|(val, delim)| match (val, delim) {
                        (Some(val), Some(delim)) => Some(substring_index_str(val, delim, n)),
                        _ => None,
                    })
                    .collect::<Utf8Array>()
                    .rename(arr.name())
            }
            None => Utf8Array::full_null(arr.name(), &DataType::Utf8, expected_size),
        },
        _ => arr_iter
            .zip(delim_iter)
            .zip(count.into_iter())
            .map(|((val, delim), n)| match (val, delim, n) {
                (Some(val), Some(delim), Some(n)) => {
                    let n: i64 = NumCast::from(n).ok_or_else(|| {
                        DaftError::ComputeError(
                            "Error in substring_index: failed to cast count as i64".to_string(),
                        )
                    })?;
                    Ok(Some(substring_index_str(val, delim, n)))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<Utf8Array>>()?
            .rename(arr.name()),
    };

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Core logic for substring_index.
fn substring_index_str(input: &str, delim: &str, count: i64) -> String {
    if count == 0 || delim.is_empty() {
        return String::new();
    }

    let parts: Vec<&str> = input.split(delim).collect();

    if count > 0 {
        let n = (count as usize).min(parts.len());
        parts[..n].join(delim)
    } else {
        let n = ((-count) as usize).min(parts.len());
        parts[parts.len() - n..].join(delim)
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Int64Array;

    use super::*;

    #[test]
    fn test_substring_index_positive() {
        let arr = Utf8Array::from_iter("a", vec![Some("www.apache.org")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(".")].into_iter());
        let count = Int64Array::from_slice("count", &[2i64]);

        let result = substring_index_impl(&arr, &delim, &count).unwrap();
        assert_eq!(result.get(0), Some("www.apache"));
    }

    #[test]
    fn test_substring_index_negative() {
        let arr = Utf8Array::from_iter("a", vec![Some("www.apache.org")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(".")].into_iter());
        let count = Int64Array::from_slice("count", &[-1i64]);

        let result = substring_index_impl(&arr, &delim, &count).unwrap();
        assert_eq!(result.get(0), Some("org"));
    }

    #[test]
    fn test_substring_index_zero() {
        let arr = Utf8Array::from_iter("a", vec![Some("www.apache.org")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(".")].into_iter());
        let count = Int64Array::from_slice("count", &[0i64]);

        let result = substring_index_impl(&arr, &delim, &count).unwrap();
        assert_eq!(result.get(0), Some(""));
    }

    #[test]
    fn test_substring_index_count_exceeds_occurrences() {
        let arr = Utf8Array::from_iter("a", vec![Some("www.apache.org")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(".")].into_iter());
        let count = Int64Array::from_slice("count", &[10i64]);

        let result = substring_index_impl(&arr, &delim, &count).unwrap();
        assert_eq!(result.get(0), Some("www.apache.org"));
    }

    #[test]
    fn test_substring_index_with_nulls() {
        let arr = Utf8Array::from_iter("a", vec![Some("a.b.c"), None, Some("x.y.z")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(".")].into_iter());
        let count = Int64Array::from_slice("count", &[2i64]);

        let result = substring_index_impl(&arr, &delim, &count).unwrap();
        assert_eq!(result.get(0), Some("a.b"));
        assert_eq!(result.get(1), None);
        assert_eq!(result.get(2), Some("x.y"));
    }
}
