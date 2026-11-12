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

/// Spark-compatible `split_part` function.
///
/// Splits string `input` on occurrences of the delimiter `delim` and returns the `part`-th part (1-based).
/// If `part` is negative, the parts are counted backward from the end of the string.
/// If `part` is out of range, an empty string is returned.
/// If `part` is 0, an error is raised.
/// If `delim` is an empty string, the input string is not split.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SplitPart;

#[typetag::serde]
impl ScalarUDF for SplitPart {
    fn name(&self) -> &'static str {
        "split_part"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let delim = inputs.required((1, "delim"))?;
        let part = inputs.required((2, "part"))?;

        input.with_utf8_array(|arr| {
            delim.with_utf8_array(|delim_arr| {
                if part.data_type().is_integer() {
                    with_match_integer_daft_types!(part.data_type(), |$T| {
                        Ok(split_part_impl(arr, delim_arr, part.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                    })
                } else if part.data_type().is_null() {
                    Ok(Series::full_null(input.name(), &DataType::Utf8, input.len()))
                } else {
                    Err(DaftError::TypeError(format!(
                        "split_part not implemented for part type {}",
                        part.data_type()
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
        ensure!(inputs.len() == 3, "split_part expects 3 arguments");
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let delim = inputs.required((1, "delim"))?.to_field(schema)?;
        let part = inputs.required((2, "part"))?.to_field(schema)?;
        ensure!(
            input.dtype.is_string(), TypeError: "Input must be of type Utf8"
        );
        ensure!(
            delim.dtype.is_string(), TypeError: "Delimiter must be of type Utf8"
        );
        ensure!(
            part.dtype.is_integer() || part.dtype.is_null(), TypeError: "Part must be of integer type, got {}", part.dtype
        );
        Ok(input)
    }

    fn docstring(&self) -> &'static str {
        "Splits the string on occurrences of the delimiter and returns the requested part (1-based). If part is negative, the parts are counted backward from the end of the string. If part is out of range, an empty string is returned. If part is 0, an error is raised. If the delimiter is an empty string, the string is not split."
    }
}

#[must_use]
pub fn split_part(input: ExprRef, delim: ExprRef, part: ExprRef) -> ExprRef {
    ScalarFn::builtin(SplitPart {}, vec![input, delim, part]).into()
}

fn split_part_impl<I>(
    arr: &Utf8Array,
    delim_arr: &Utf8Array,
    part: &DataArray<I>,
) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord + std::hash::Hash,
{
    let (is_full_null, expected_size) = parse_inputs(arr, &[delim_arr])
        .map_err(|e| DaftError::ValueError(format!("Error in split_part: {e}")))?;
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
        part.len() == 1 || part.len() == expected_size,
        ComputeError: "split_part: part array length ({}) is not broadcastable to expected size ({})",
        part.len(),
        expected_size
    );

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    let delim_iter = create_broadcasted_str_iter(delim_arr, expected_size);

    let result: Utf8Array = match part.len() {
        1 => match part.get(0) {
            Some(n) => {
                let n: i64 = NumCast::from(n).ok_or_else(|| {
                    DaftError::ComputeError(
                        "Error in split_part: failed to cast part as i64".to_string(),
                    )
                })?;
                ensure!(n != 0, ValueError: "split_part: part must not be 0");
                arr_iter
                    .zip(delim_iter)
                    .map(|(val, delim)| match (val, delim) {
                        (Some(val), Some(delim)) => Some(split_part_str(val, delim, n)),
                        _ => None,
                    })
                    .collect::<Utf8Array>()
                    .rename(arr.name())
            }
            None => Utf8Array::full_null(arr.name(), &DataType::Utf8, expected_size),
        },
        _ => arr_iter
            .zip(delim_iter)
            .zip(part.into_iter())
            .map(|((val, delim), n)| match (val, delim, n) {
                (Some(val), Some(delim), Some(n)) => {
                    let n: i64 = NumCast::from(n).ok_or_else(|| {
                        DaftError::ComputeError(
                            "Error in split_part: failed to cast part as i64".to_string(),
                        )
                    })?;
                    ensure!(n != 0, ValueError: "split_part: part must not be 0");
                    Ok(Some(split_part_str(val, delim, n)))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<Utf8Array>>()?
            .rename(arr.name()),
    };

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// Core logic for split_part. `part` must be non-zero (validated by the caller).
fn split_part_str(input: &str, delim: &str, part: i64) -> String {
    // Spark and Postgres treat an empty delimiter as "do not split".
    let parts: Vec<&str> = if delim.is_empty() {
        vec![input]
    } else {
        input.split(delim).collect()
    };

    let idx = if part > 0 {
        (part - 1) as usize
    } else {
        // Use `unsigned_abs` to avoid `(-part) as usize` overflowing at `i64::MIN`.
        let abs = part.unsigned_abs() as usize;
        if abs > parts.len() {
            return String::new();
        }
        parts.len() - abs
    };

    parts.get(idx).map_or_else(String::new, ToString::to_string)
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Int64Array;

    use super::*;

    #[test]
    fn test_split_part_positive() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[2i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("b"));
    }

    #[test]
    fn test_split_part_negative() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[-1i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("c"));
    }

    #[test]
    fn test_split_part_out_of_range() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[10i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some(""));
    }

    #[test]
    fn test_split_part_negative_out_of_range() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[-10i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some(""));
    }

    #[test]
    fn test_split_part_zero_errors() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[0i64]);

        let result = split_part_impl(&arr, &delim, &part);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_part_delimiter_not_found() {
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[1i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("abc"));
    }

    #[test]
    fn test_split_part_empty_delimiter() {
        let arr = Utf8Array::from_iter("a", vec![Some("abc")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some("")].into_iter());
        let part = Int64Array::from_slice("part", &[1i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("abc"));

        let part = Int64Array::from_slice("part", &[2i64]);
        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some(""));
    }

    #[test]
    fn test_split_part_trailing_delimiter() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[3i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some(""));
    }

    #[test]
    fn test_split_part_with_nulls() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c"), None, Some("x,y,z")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[2i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("b"));
        assert_eq!(result.get(1), None);
        assert_eq!(result.get(2), Some("y"));
    }

    #[test]
    fn test_split_part_per_row_parts() {
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c"), Some("x,y,z")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[1i64, -1i64]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some("a"));
        assert_eq!(result.get(1), Some("z"));
    }

    #[test]
    fn test_split_part_i64_min_does_not_overflow() {
        // `(-part) as usize` would overflow at `i64::MIN`; using `unsigned_abs`
        // sidesteps this. The expected behavior is the same as any other
        // out-of-range negative part: return an empty string.
        let arr = Utf8Array::from_iter("a", vec![Some("a,b,c")].into_iter());
        let delim = Utf8Array::from_iter("delim", vec![Some(",")].into_iter());
        let part = Int64Array::from_slice("part", &[i64::MIN]);

        let result = split_part_impl(&arr, &delim, &part).unwrap();
        assert_eq!(result.get(0), Some(""));
    }
}
