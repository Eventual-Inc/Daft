// ****************************************************************************************
// IMPLEMENTER NOTE: This file will soon be deprecated and functionality should be moved to
// src/array/ops/utf8.rs instead
// ****************************************************************************************

use arrow2::array::Utf8Array;

use common_error::{DaftError, DaftResult};

fn concat_strings(l: &str, r: &str) -> String {
    // fastest way to concat strings according to https://github.com/hoodie/concatenation_benchmarks-rs
    let mut s = String::with_capacity(l.len() + r.len());
    s.push_str(l);
    s.push_str(r);
    s
}

fn add_utf8_scalar(arr: &Utf8Array<i64>, other: &str) -> DaftResult<Utf8Array<i64>> {
    let result: Utf8Array<i64> = arr
        .into_iter()
        .map(|v| v.map(|v| concat_strings(v, other)))
        .collect();
    Ok(result)
}

fn add_scalar_utf8(prefix: &str, arr: &Utf8Array<i64>) -> DaftResult<Utf8Array<i64>> {
    let result: Utf8Array<i64> = arr
        .into_iter()
        .map(|v| v.map(|v| concat_strings(prefix, v)))
        .collect();
    Ok(result)
}

pub fn add_utf8_arrays(lhs: &Utf8Array<i64>, rhs: &Utf8Array<i64>) -> DaftResult<Utf8Array<i64>> {
    if rhs.len() == 1 {
        let is_valid = match rhs.validity() {
            Some(validity) => validity.get_bit(0),
            None => true,
        };

        return match is_valid {
            true => add_utf8_scalar(lhs, rhs.value(0)),
            false => Ok(Utf8Array::new_null(
                arrow2::datatypes::DataType::LargeUtf8,
                lhs.len(),
            )),
        };
    }
    if lhs.len() == 1 {
        let is_valid = match lhs.validity() {
            Some(validity) => validity.get_bit(0),
            None => true,
        };

        return match is_valid {
            true => add_scalar_utf8(lhs.value(0), rhs),
            false => Ok(Utf8Array::new_null(
                arrow2::datatypes::DataType::LargeUtf8,
                rhs.len(),
            )),
        };
    }

    if lhs.len() != rhs.len() {
        return Err(DaftError::ComputeError(format!(
            "lhs and rhs have different length arrays: {} vs {}",
            lhs.len(),
            rhs.len()
        )));
    }

    let result: Utf8Array<i64> = lhs
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
        let l = Utf8Array::<i64>::from(vec!["a".into(), "b".into(), "c".into()]);
        let r = Utf8Array::<i64>::from(vec!["1".into(), "2".into(), "3".into()]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "b2");
        assert_eq!(result.value(2), "c3");
        Ok(())
    }

    #[test]
    fn check_add_utf_arrays_broadcast_left() -> DaftResult<()> {
        let l = Utf8Array::<i64>::from(vec!["a".into()]);
        let r = Utf8Array::<i64>::from(vec!["1".into(), "2".into(), "3".into()]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "a2");
        assert_eq!(result.value(2), "a3");
        Ok(())
    }

    #[test]
    fn check_add_utf_arrays_broadcast_right() -> DaftResult<()> {
        let l = Utf8Array::<i64>::from(vec!["a".into(), "b".into(), "c".into()]);
        let r = Utf8Array::<i64>::from(vec!["1".into()]);
        let result = add_utf8_arrays(&l, &r)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "a1");
        assert_eq!(result.value(1), "b1");
        assert_eq!(result.value(2), "c1");
        Ok(())
    }
}
