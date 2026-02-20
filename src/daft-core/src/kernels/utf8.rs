// ****************************************************************************************
// IMPLEMENTER NOTE: This file will soon be deprecated and functionality should be moved to
// src/array/ops/utf8.rs instead
// ****************************************************************************************

use arrow::array::{Array, LargeStringArray};
use common_error::{DaftError, DaftResult};

fn concat_strings(l: &str, r: &str) -> String {
    // fastest way to concat strings according to https://github.com/hoodie/concatenation_benchmarks-rs
    let mut s = String::with_capacity(l.len() + r.len());
    s.push_str(l);
    s.push_str(r);
    s
}

fn add_utf8_scalar(arr: &LargeStringArray, other: &str) -> DaftResult<LargeStringArray> {
    let result: LargeStringArray = arr
        .into_iter()
        .map(|v| v.map(|v| concat_strings(v, other)))
        .collect();
    Ok(result)
}

fn add_scalar_utf8(prefix: &str, arr: &LargeStringArray) -> DaftResult<LargeStringArray> {
    let result: LargeStringArray = arr
        .into_iter()
        .map(|v| v.map(|v| concat_strings(prefix, v)))
        .collect();
    Ok(result)
}

pub fn add_utf8_arrays(
    lhs: &LargeStringArray,
    rhs: &LargeStringArray,
) -> DaftResult<LargeStringArray> {
    if rhs.len() == 1 {
        let is_valid = match rhs.nulls() {
            Some(nulls) => nulls.is_valid(0),
            None => true,
        };

        return match is_valid {
            true => add_utf8_scalar(lhs, rhs.value(0)),
            false => Ok(LargeStringArray::new_null(lhs.len())),
        };
    }
    if lhs.len() == 1 {
        let is_valid = match lhs.nulls() {
            Some(nulls) => nulls.is_valid(0),
            None => true,
        };

        return match is_valid {
            true => add_scalar_utf8(lhs.value(0), rhs),
            false => Ok(LargeStringArray::new_null(rhs.len())),
        };
    }

    if lhs.len() != rhs.len() {
        return Err(DaftError::ComputeError(format!(
            "lhs and rhs have different length arrays: {} vs {}",
            lhs.len(),
            rhs.len()
        )));
    }

    let result: LargeStringArray = lhs
        .into_iter()
        .zip(rhs)
        .map(|(l, r)| match (l, r) {
            (Some(l), Some(r)) => Some(concat_strings(l, r)),
            _ => None,
        })
        .collect();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_add_utf_arrays() -> DaftResult<()> {
        let l = LargeStringArray::from_iter_values(vec!["a", "b", "c"]);
        let r = LargeStringArray::from_iter_values(vec!["1", "2", "3"]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "b2");
        assert_eq!(result.value(2), "c3");
        Ok(())
    }

    #[test]
    fn check_add_utf_arrays_broadcast_left() -> DaftResult<()> {
        let l = LargeStringArray::from_iter_values(vec!["a"]);
        let r = LargeStringArray::from_iter_values(vec!["1", "2", "3"]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "a2");
        assert_eq!(result.value(2), "a3");
        Ok(())
    }

    #[test]
    fn check_add_utf_arrays_broadcast_right() -> DaftResult<()> {
        let l = LargeStringArray::from_iter_values(vec!["a", "b", "c"]);
        let r = LargeStringArray::from_iter_values(vec!["1"]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "b1");
        assert_eq!(result.value(2), "c1");
        Ok(())
    }
}
