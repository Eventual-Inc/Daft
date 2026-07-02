use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, FullNull, Int64Array, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Overlay;

#[typetag::serde]
impl ScalarUDF for Overlay {
    fn name(&self) -> &'static str {
        "overlay"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        ensure!(
            inputs.len() == 3 || inputs.len() == 4,
            ComputeError: "overlay expects 3 or 4 arguments, got {}",
            inputs.len()
        );

        let src = inputs.required((0, "src"))?;
        let replace = inputs.required((1, "replace"))?;
        let pos = inputs.required((2, "pos"))?;
        let opt_len = inputs.optional((3, "len"))?;

        ensure!(
            *src.data_type() == DataType::Utf8 || src.data_type().is_null(),
            TypeError: "src must be a string, got {}", src.data_type()
        );
        ensure!(
            *replace.data_type() == DataType::Utf8 || replace.data_type().is_null(),
            TypeError: "replace must be a string, got {}", replace.data_type()
        );
        ensure!(
            pos.data_type().is_integer() || pos.data_type().is_null(),
            TypeError: "pos must be an integer, got {}", pos.data_type()
        );
        if let Some(l) = opt_len {
            ensure!(
                l.data_type().is_integer() || l.data_type().is_null(),
                TypeError: "len must be an integer, got {}", l.data_type()
            );
        }

        let pos = pos.cast(&DataType::Int64)?;
        let len = match opt_len {
            Some(l) => Some(l.cast(&DataType::Int64)?),
            None => None,
        };

        src.with_utf8_array(|src_arr| {
            replace.with_utf8_array(|replace_arr| {
                let pos_arr = pos.i64()?;
                let len_arr = match &len {
                    Some(l) => Some(l.i64()?),
                    None => None,
                };
                Ok(overlay_impl(src_arr, replace_arr, pos_arr, len_arr)?.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 3 || inputs.len() == 4,
            SchemaMismatch: "overlay expects 3 or 4 arguments, got {}",
            inputs.len()
        );

        let src = inputs.required((0, "src"))?.to_field(schema)?;
        let replace = inputs.required((1, "replace"))?.to_field(schema)?;
        let pos = inputs.required((2, "pos"))?.to_field(schema)?;

        ensure!(
            src.dtype == DataType::Utf8 || src.dtype == DataType::Null,
            TypeError: "src must be a string, got {}", src.dtype
        );
        ensure!(
            replace.dtype == DataType::Utf8 || replace.dtype == DataType::Null,
            TypeError: "replace must be a string, got {}", replace.dtype
        );
        ensure!(
            pos.dtype.is_integer() || pos.dtype == DataType::Null,
            TypeError: "pos must be an integer, got {}", pos.dtype
        );

        if let Some(len) = inputs.optional((3, "len"))? {
            let len = len.to_field(schema)?;
            ensure!(
                len.dtype.is_integer() || len.dtype == DataType::Null,
                TypeError: "len must be an integer, got {}", len.dtype
            );
        }

        Ok(src)
    }

    fn docstring(&self) -> &'static str {
        "Overlays the replace string onto the src string starting at position pos, replacing len characters (defaults to the length of replace)."
    }
}

#[must_use]
pub fn overlay(src: ExprRef, replace: ExprRef, pos: ExprRef, len: ExprRef) -> ExprRef {
    ScalarFn::builtin(Overlay {}, vec![src, replace, pos, len]).into()
}

fn overlay_impl(
    src_arr: &Utf8Array,
    replace_arr: &Utf8Array,
    pos_arr: &Int64Array,
    len_arr: Option<&Int64Array>,
) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(src_arr, &[replace_arr])
        .map_err(|e| DaftError::ValueError(format!("Error in overlay: {e}")))?;
    if is_full_null {
        return Ok(Utf8Array::full_null(
            src_arr.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Utf8Array::empty(src_arr.name(), &DataType::Utf8));
    }

    let src_iter = create_broadcasted_str_iter(src_arr, expected_size);
    let replace_iter = create_broadcasted_str_iter(replace_arr, expected_size);

    let pos_vals = broadcast_i64(pos_arr, expected_size)?;
    let len_vals = match len_arr {
        Some(l) => Some(broadcast_i64(l, expected_size)?),
        None => None,
    };

    let result: Utf8Array = src_iter
        .zip(replace_iter)
        .enumerate()
        .map(|(i, (s, r))| {
            let p = pos_vals.get(i).copied().flatten();
            let l = len_vals.as_ref().and_then(|v| v.get(i).copied().flatten());
            match (s, r, p) {
                (Some(s), Some(r), Some(p)) => Ok(Some(overlay_str(s, r, p, l))),
                _ => Ok(None),
            }
        })
        .collect::<DaftResult<Utf8Array>>()?
        .rename(src_arr.name());

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

fn broadcast_i64(arr: &Int64Array, expected_size: usize) -> DaftResult<Vec<Option<i64>>> {
    ensure!(
        arr.len() == 1 || arr.len() == expected_size,
        ComputeError: "overlay: argument length ({}) is not broadcastable to expected size ({})",
        arr.len(),
        expected_size
    );

    if arr.len() == 1 {
        let v = arr.get(0);
        Ok(std::iter::repeat_n(v, expected_size).collect())
    } else {
        Ok((0..expected_size).map(|i| arr.get(i)).collect())
    }
}

fn overlay_str(src: &str, replace: &str, pos: i64, len: Option<i64>) -> String {
    let pos = pos.max(1) as usize;
    let replace_chars: Vec<char> = replace.chars().collect();
    let src_chars: Vec<char> = src.chars().collect();

    let len = match len {
        Some(l) => l.max(0) as usize,
        None => replace_chars.len(),
    };

    let start = (pos - 1).min(src_chars.len());
    let end = (start + len).min(src_chars.len());

    let mut result: String = src_chars[..start].iter().collect();
    result.extend(replace_chars);
    result.extend(src_chars[end..].iter());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlay_basic() {
        assert_eq!(overlay_str("AAAAAAAAAA", "RR", 5, None), "AAAARRAAAA");
    }

    #[test]
    fn test_overlay_explicit_len_matches_replace() {
        assert_eq!(overlay_str("AAAAAAAAAA", "RR", 5, Some(2)), "AAAARRAAAA");
    }

    #[test]
    fn test_overlay_len_smaller_than_replace() {
        assert_eq!(overlay_str("AAAAAAAAAA", "RRR", 5, Some(1)), "AAAARRRAAAAA");
    }

    #[test]
    fn test_overlay_len_larger_than_replace() {
        assert_eq!(overlay_str("AAAAAAAAAA", "RR", 5, Some(4)), "AAAARRAA");
    }

    #[test]
    fn test_overlay_pos_beyond_end() {
        assert_eq!(overlay_str("AAAA", "RR", 10, None), "AAAARR");
    }

    #[test]
    fn test_overlay_pos_clamped_to_min_one() {
        assert_eq!(overlay_str("AAAA", "RR", 0, None), "RRAA");
    }

    #[test]
    fn test_overlay_empty_replace() {
        assert_eq!(overlay_str("AAAA", "", 2, None), "AAAA");
    }

    #[test]
    fn test_overlay_empty_src() {
        assert_eq!(overlay_str("", "RR", 1, None), "RR");
    }
}
