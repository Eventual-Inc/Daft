use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::utils::create_broadcasted_str_iter;

/// Spark-compatible `overlay(input, replace, pos[, len])`.
///
/// Replaces the substring of `input` starting at `pos` (1-based) with `replace`.
/// `len` controls how many characters of the original string are removed; when
/// `len` is negative or omitted, the length of `replace` is used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Overlay;

#[derive(FunctionArgs)]
struct OverlayArgs<T> {
    input: T,
    replace: T,
    pos: T,
    #[arg(optional)]
    len: Option<T>,
}

fn compute_overlay(input: &str, replace: &str, pos: i64, len: Option<i64>) -> String {
    // Spark normalizes pos < 1 to 1.
    let pos_one_based = pos.max(1) as usize;
    // When len is omitted or negative, fall back to char length of `replace`.
    let replace_char_len = replace.chars().count() as i64;
    let effective_len = match len {
        Some(l) if l >= 0 => l,
        _ => replace_char_len,
    } as usize;

    let input_chars: Vec<char> = input.chars().collect();
    let total = input_chars.len();

    let start_idx = (pos_one_based - 1).min(total);
    let end_idx = start_idx.saturating_add(effective_len).min(total);

    let mut result = String::with_capacity(input.len() + replace.len());
    for c in &input_chars[..start_idx] {
        result.push(*c);
    }
    result.push_str(replace);
    for c in &input_chars[end_idx..] {
        result.push(*c);
    }
    result
}

#[typetag::serde]
impl ScalarUDF for Overlay {
    fn name(&self) -> &'static str {
        "overlay"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let OverlayArgs {
            input,
            replace,
            pos,
            len,
        } = inputs.try_into()?;

        let input = input.cast(&DataType::Utf8)?;
        let replace = replace.cast(&DataType::Utf8)?;
        let pos = pos.cast(&DataType::Int64)?;
        let len_series = match len {
            Some(s) => Some(s.cast(&DataType::Int64)?),
            None => None,
        };
        let name = input.name();

        input.with_utf8_array(|input_arr| {
            replace.with_utf8_array(|replace_arr| {
                let pos_arr = pos.i64()?;
                let mut other_lens: Vec<usize> = vec![replace_arr.len(), pos_arr.len()];
                if let Some(ref l) = len_series {
                    other_lens.push(l.len());
                }

                // Determine broadcast length manually (parse_inputs only handles same DaftPhysicalType).
                let expected_size = compute_broadcast_len(input_arr.len(), &other_lens)
                    .map_err(|e| DaftError::ValueError(format!("Error in overlay: {e}")))?;

                if expected_size == 0 {
                    return Ok(Utf8Array::empty(name, &DataType::Utf8).into_series());
                }

                // If any input is full-null, return all nulls.
                let any_full_null = input_arr.null_count() == input_arr.len()
                    || replace_arr.null_count() == replace_arr.len()
                    || pos_arr.null_count() == pos_arr.len();
                if any_full_null {
                    return Ok(
                        Utf8Array::full_null(name, &DataType::Utf8, expected_size).into_series()
                    );
                }

                let input_iter = create_broadcasted_str_iter(input_arr, expected_size);
                let replace_iter = create_broadcasted_str_iter(replace_arr, expected_size);

                // Build broadcasted iterators for pos / len ourselves.
                let pos_vec: Vec<Option<i64>> = broadcast_numeric(pos_arr, expected_size)?;
                let len_vec: Vec<Option<i64>> = match len_series.as_ref() {
                    Some(s) => broadcast_numeric(s.i64()?, expected_size)?,
                    None => vec![None; expected_size],
                };

                let result: Utf8Array = input_iter
                    .zip(replace_iter)
                    .zip(pos_vec.into_iter().zip(len_vec.into_iter()))
                    .map(|((i, r), (p, l))| match (i, r, p) {
                        (Some(i), Some(r), Some(p)) => Some(compute_overlay(i, r, p, l)),
                        _ => None,
                    })
                    .collect::<Utf8Array>()
                    .rename(name);

                Ok(result.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let OverlayArgs {
            input,
            replace,
            pos,
            len,
        } = inputs.try_into()?;

        let input = input.to_field(schema)?;
        let replace = replace.to_field(schema)?;
        let pos = pos.to_field(schema)?;

        ensure!(
            input.dtype.is_string() || input.dtype == DataType::Null,
            TypeError: "Expected input to 'overlay' to be utf8, got {}", input.dtype
        );
        ensure!(
            replace.dtype.is_string() || replace.dtype == DataType::Null,
            TypeError: "Expected 'replace' argument of 'overlay' to be utf8, got {}", replace.dtype
        );
        ensure!(
            pos.dtype.is_integer() || pos.dtype == DataType::Null,
            TypeError: "Expected 'pos' argument of 'overlay' to be integer, got {}", pos.dtype
        );
        if let Some(len_expr) = len {
            let len_field = len_expr.to_field(schema)?;
            ensure!(
                len_field.dtype.is_integer() || len_field.dtype == DataType::Null,
                TypeError: "Expected 'len' argument of 'overlay' to be integer, got {}", len_field.dtype
            );
        }

        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Replaces the substring of `input` starting at `pos` (1-based) for `len` characters \
        with `replace`. When `len` is omitted or negative, the length of `replace` is used."
    }
}

#[must_use]
pub fn overlay(input: ExprRef, replace: ExprRef, pos: ExprRef, len: Option<ExprRef>) -> ExprRef {
    let mut args = vec![input, replace, pos];
    if let Some(len) = len {
        args.push(len);
    }
    ScalarFn::builtin(Overlay, args).into()
}

fn compute_broadcast_len(input_len: usize, others: &[usize]) -> Result<usize, String> {
    if input_len == 0 {
        return Ok(0);
    }
    let mut result_len = input_len;
    for &l in others {
        if l == 0 {
            return Ok(0);
        }
        if l == 1 {
            continue;
        }
        if result_len == 1 {
            result_len = l;
        } else if result_len != l {
            return Err(format!(
                "broadcast length mismatch: {input_len} vs {others:?}"
            ));
        }
    }
    Ok(result_len)
}

fn broadcast_numeric(
    arr: &daft_core::prelude::Int64Array,
    expected_size: usize,
) -> DaftResult<Vec<Option<i64>>> {
    let len = arr.len();
    if len == 1 {
        let v = arr.get(0).and_then(|x| x.to_i64());
        Ok(vec![v; expected_size])
    } else if len == expected_size {
        Ok(arr.into_iter().collect())
    } else {
        Err(DaftError::ValueError(format!(
            "broadcast length mismatch: got {len}, expected {expected_size} or 1"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlay_basic() {
        // Spark example: overlay('Spark SQL', '_', 6) -> 'Spark_SQL'
        assert_eq!(compute_overlay("Spark SQL", "_", 6, None), "Spark_SQL");
        // overlay('Spark SQL', 'CORE', 7) -> 'Spark CORE'
        assert_eq!(compute_overlay("Spark SQL", "CORE", 7, None), "Spark CORE");
        // overlay('Spark SQL', 'ANSI ', 7, 0) -> 'Spark ANSI SQL'
        assert_eq!(
            compute_overlay("Spark SQL", "ANSI ", 7, Some(0)),
            "Spark ANSI SQL"
        );
        // overlay('Spark SQL', 'tructured', 2, 4) -> 'Structured SQL'
        assert_eq!(
            compute_overlay("Spark SQL", "tructured", 2, Some(4)),
            "Structured SQL"
        );
    }

    #[test]
    fn test_overlay_pos_clamp() {
        // pos < 1 is normalized to 1.
        assert_eq!(compute_overlay("abcdef", "X", 0, Some(1)), "Xbcdef");
        assert_eq!(compute_overlay("abcdef", "X", -3, Some(2)), "Xcdef");
    }

    #[test]
    fn test_overlay_negative_len() {
        // Negative len falls back to replace length.
        assert_eq!(compute_overlay("abcdef", "XYZ", 2, Some(-1)), "aXYZef");
    }

    #[test]
    fn test_overlay_pos_beyond() {
        // pos beyond string length: append at end without removing chars.
        assert_eq!(compute_overlay("abc", "XYZ", 10, None), "abcXYZ");
    }

    #[test]
    fn test_overlay_unicode() {
        assert_eq!(compute_overlay("αβγδε", "X", 3, Some(1)), "αβXδε");
    }
}
